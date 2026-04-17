import {
  ActionButton,
  ActionCluster,
  CheckboxField,
  EmptyState,
  EntityTable,
  InlineNotice,
  TextInputField,
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  PrettyJsonBlock,
  formatUnixMs,
  readNumber,
  readObject,
  readString,
  toStringArray,
  type JsonObject,
} from "../shared";
import { pseudoLocalizeText } from "../i18n";
import type { ConsoleAppState } from "../useConsoleAppState";

type SupportSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "supportBusy"
    | "supportDeployment"
    | "supportDiagnosticsSnapshot"
    | "supportBundleRetainJobs"
    | "setSupportBundleRetainJobs"
    | "supportBundleJobs"
    | "supportSelectedBundleJobId"
    | "setSupportSelectedBundleJobId"
    | "supportSelectedBundleJob"
    | "supportDoctorRetainJobs"
    | "setSupportDoctorRetainJobs"
    | "supportDoctorOnly"
    | "setSupportDoctorOnly"
    | "supportDoctorSkip"
    | "setSupportDoctorSkip"
    | "supportDoctorRollbackRunId"
    | "setSupportDoctorRollbackRunId"
    | "supportDoctorForce"
    | "setSupportDoctorForce"
    | "supportDoctorJobs"
    | "supportSelectedDoctorJobId"
    | "setSupportSelectedDoctorJobId"
    | "supportSelectedDoctorJob"
    | "refreshSupport"
    | "createSupportBundle"
    | "loadSupportBundleJob"
    | "queueDoctorRecoveryPreview"
    | "queueDoctorRecoveryApply"
    | "queueDoctorRollbackPreview"
    | "queueDoctorRollbackApply"
    | "loadDoctorRecoveryJob"
    | "setSection"
    | "locale"
    | "revealSensitiveValues"
  >;
};

type SupportJobRow = {
  jobId: string;
  state: string;
  requestedAt: string;
};

const SUPPORT_MESSAGES = {
  "header.title": "Support",
  "header.heading": "Support and Recovery",
  "header.description":
    "Queue support bundles, inspect queued doctor recovery plans, and move into rollback or diagnostics without leaving the dashboard.",
  "status.failedBundleJobs": "failed bundle jobs",
  "status.failedRecoveryJobs": "failed recovery jobs",
  "status.deploymentWarnings": "deployment warnings",
  "status.noRecentFailure": "No recent failure",
  "status.recentFailurePublished": "Recent failure published",
  "action.refresh": "Refresh support",
  "action.refreshing": "Refreshing...",
  "metric.supportQueue": "Support queue",
  "metric.noQueuedJobs": "No queued jobs",
  "metric.recoveryQueue": "Recovery queue",
  "metric.noRecoveryJobs": "No recovery jobs",
  "metric.modeUnavailable": "mode unavailable",
  "metric.bundleReliability": "Bundle reliability",
  "metric.attempts": "{count} attempts",
  "metric.deploymentPosture": "Deployment posture",
  "metric.modeUnavailableLong": "Mode unavailable",
  "bundle.title": "Queue support bundle",
  "bundle.description":
    "Support bundle work remains queue-backed so command execution survives browser disconnects.",
  "bundle.retainJobs": "Retain jobs",
  "bundle.queue": "Queue support bundle",
  "bundle.queueing": "Queueing...",
  "bundle.openDiagnostics": "Open diagnostics",
  "bundle.openConfig": "Open config",
  "bundle.currentWarnings": "Current warnings",
  "recoveryPlanner.title": "Doctor recovery planner",
  "recoveryPlanner.description":
    "Queue repair previews, apply changes, or rehearse rollback against a recorded recovery run.",
  "recoveryPlanner.retainJobs": "Retain recovery jobs",
  "recoveryPlanner.rollbackRunId": "Rollback run ID",
  "recoveryPlanner.rollbackRunDescription": "Required only for rollback preview/apply.",
  "recoveryPlanner.onlyChecks": "Only checks",
  "recoveryPlanner.checkDescription": "Comma or newline separated doctor step IDs.",
  "recoveryPlanner.skipChecks": "Skip checks",
  "recoveryPlanner.force": "Force destructive recovery paths",
  "recoveryPlanner.forceDescription":
    "Needed only when rollback hash validation or destructive repair steps require explicit operator acknowledgement.",
  "recoveryPlanner.queuePreview": "Queue preview",
  "recoveryPlanner.applyRepairs": "Apply repairs",
  "recoveryPlanner.previewRollback": "Preview rollback",
  "recoveryPlanner.applyRollback": "Apply rollback",
  "recoveryPlanner.latestJob": "Latest recovery job",
  "signals.title": "Recent degraded signals",
  "signals.description": "Keep the latest failure classes and messages close to support actions.",
  "signals.emptyTitle": "No recent failures",
  "signals.emptyDescription": "No recent failures published by diagnostics.",
  "signals.unknownFailure": "Unknown failure",
  "signals.operationUnavailable": "Operation unavailable",
  "signals.noRedactedMessage": "No redacted message published.",
  "provider.title": "Provider auth recovery",
  "provider.description":
    "Keep provider-auth degradation and next recovery motion visible next to support workflows.",
  "provider.degradedProfiles": "degraded profiles",
  "provider.body":
    "Recovery stays explicit: move into diagnostics for current failures or auth/config settings when profile posture needs operator intervention.",
  "provider.openDiagnostics": "Open diagnostics",
  "provider.openAuthProfiles": "Open auth profiles",
  "playbook.title": "Triage playbook",
  "playbook.description":
    "Keep the support handoff order visible so the dashboard stays the primary recovery surface.",
  "playbook.step1": "Check deployment warnings and provider auth state.",
  "playbook.step2": "Queue a doctor preview before applying repair or rollback.",
  "playbook.step3": "Load the latest support bundle and recovery jobs to inspect command output.",
  "playbook.step4": "Inspect diagnostics before changing config or auth posture.",
  "playbook.reference": "Reference",
  "summary.title": "Latest recovery summary",
  "summary.description": "Surface the latest published doctor summary directly from diagnostics.",
  "summary.emptyTitle": "No recovery summary published",
  "summary.emptyDescription": "Queue a doctor preview to populate recovery telemetry.",
  "bundleJobs.title": "Queued bundle jobs",
  "bundleJobs.description":
    "Support bundle jobs remain visible after completion so operators can verify output paths and failure reasons.",
  "table.job": "Job",
  "table.state": "State",
  "table.actions": "Actions",
  "table.requested": "requested {value}",
  "table.select": "Select",
  "bundleJobs.emptyTitle": "No support bundle jobs queued",
  "bundleJobs.emptyDescription":
    "Queue a support bundle to inspect command output and artifact paths.",
  "selectedBundle.title": "Selected bundle job",
  "selectedBundle.description":
    "Load command output, output path, and failure detail for the chosen support bundle job.",
  "selectedBundle.load": "Load job",
  "selectedBundle.loading": "Loading...",
  "selectedBundle.jobId": "Job ID",
  "selectedBundle.emptyTitle": "No support bundle job selected",
  "selectedBundle.emptyDescription": "Select a job and load it to inspect details.",
  "recoveryJobs.title": "Recovery jobs",
  "recoveryJobs.description":
    "Queue-backed doctor runs keep preview/apply/rollback history visible after the browser disconnects.",
  "recoveryJobs.emptyTitle": "No recovery jobs queued",
  "recoveryJobs.emptyDescription":
    "Queue a doctor preview or rollback rehearsal to inspect the recovery plan.",
  "selectedRecovery.title": "Selected recovery job",
  "selectedRecovery.description":
    "Load the selected doctor job to inspect parsed recovery output, available rollback runs, and command stderr/stdout.",
  "selectedRecovery.load": "Load recovery job",
  "selectedRecovery.jobId": "Recovery job ID",
  "selectedRecovery.emptyTitle": "No recovery job selected",
  "selectedRecovery.emptyDescription":
    "Select a doctor recovery job and load it to inspect details.",
  "selectedRecovery.summary": "Recovery summary",
} as const;

type SupportMessageKey = keyof typeof SUPPORT_MESSAGES;

const SUPPORT_MESSAGES_CS: Readonly<Record<SupportMessageKey, string>> = {
  "header.title": "Podpora",
  "header.heading": "Podpora a obnova",
  "header.description":
    "Zařazuj support bundle, kontroluj naplánované recovery plány doctoru a přecházej do rollbacku nebo diagnostiky bez opuštění dashboardu.",
  "status.failedBundleJobs": "selhané bundle joby",
  "status.failedRecoveryJobs": "selhané recovery joby",
  "status.deploymentWarnings": "varování nasazení",
  "status.noRecentFailure": "Žádné nedávné selhání",
  "status.recentFailurePublished": "Nedávné selhání publikováno",
  "action.refresh": "Obnovit podporu",
  "action.refreshing": "Obnovuji...",
  "metric.supportQueue": "Fronta podpory",
  "metric.noQueuedJobs": "Žádné zařazené joby",
  "metric.recoveryQueue": "Fronta obnovy",
  "metric.noRecoveryJobs": "Žádné recovery joby",
  "metric.modeUnavailable": "režim není dostupný",
  "metric.bundleReliability": "Spolehlivost bundle",
  "metric.attempts": "{count} pokusů",
  "metric.deploymentPosture": "Postura nasazení",
  "metric.modeUnavailableLong": "Režim není dostupný",
  "bundle.title": "Zařadit support bundle",
  "bundle.description":
    "Práce se support bundle zůstává frontovaná, aby spuštění příkazů přežilo odpojení prohlížeče.",
  "bundle.retainJobs": "Ponechat joby",
  "bundle.queue": "Zařadit support bundle",
  "bundle.queueing": "Zařazuji...",
  "bundle.openDiagnostics": "Otevřít diagnostiku",
  "bundle.openConfig": "Otevřít konfiguraci",
  "bundle.currentWarnings": "Aktuální varování",
  "recoveryPlanner.title": "Plánovač doctor recovery",
  "recoveryPlanner.description":
    "Zařazuj preview oprav, aplikuj změny nebo si nanečisto vyzkoušej rollback proti zaznamenanému recovery běhu.",
  "recoveryPlanner.retainJobs": "Ponechat recovery joby",
  "recoveryPlanner.rollbackRunId": "ID rollback běhu",
  "recoveryPlanner.rollbackRunDescription": "Vyžadováno jen pro preview/aplikaci rollbacku.",
  "recoveryPlanner.onlyChecks": "Pouze kontroly",
  "recoveryPlanner.checkDescription": "ID kroků doctoru oddělená čárkou nebo novým řádkem.",
  "recoveryPlanner.skipChecks": "Přeskočit kontroly",
  "recoveryPlanner.force": "Vynutit destruktivní recovery cesty",
  "recoveryPlanner.forceDescription":
    "Potřebné jen tehdy, když validace rollback hashe nebo destruktivní opravné kroky vyžadují explicitní potvrzení operátorem.",
  "recoveryPlanner.queuePreview": "Zařadit preview",
  "recoveryPlanner.applyRepairs": "Aplikovat opravy",
  "recoveryPlanner.previewRollback": "Preview rollbacku",
  "recoveryPlanner.applyRollback": "Aplikovat rollback",
  "recoveryPlanner.latestJob": "Poslední recovery job",
  "signals.title": "Nedávné degradované signály",
  "signals.description": "Drž nejnovější třídy selhání a zprávy blízko podpůrným akcím.",
  "signals.emptyTitle": "Žádná nedávná selhání",
  "signals.emptyDescription": "Diagnostika nezveřejnila žádná nedávná selhání.",
  "signals.unknownFailure": "Neznámé selhání",
  "signals.operationUnavailable": "Operace není dostupná",
  "signals.noRedactedMessage": "Nebyla publikována žádná redigovaná zpráva.",
  "provider.title": "Obnova provider auth",
  "provider.description":
    "Drž degradaci provider auth a další recovery krok viditelný vedle podpůrných workflow.",
  "provider.degradedProfiles": "degradované profily",
  "provider.body":
    "Obnova zůstává explicitní: přejdi do diagnostiky pro aktuální selhání nebo do auth/config nastavení, když postura profilu vyžaduje zásah operátora.",
  "provider.openDiagnostics": "Otevřít diagnostiku",
  "provider.openAuthProfiles": "Otevřít auth profily",
  "playbook.title": "Triage playbook",
  "playbook.description":
    "Drž pořadí support handoffu viditelné, aby dashboard zůstal hlavní recovery surface.",
  "playbook.step1": "Zkontroluj varování nasazení a stav provider auth.",
  "playbook.step2": "Zařaď doctor preview před aplikací opravy nebo rollbacku.",
  "playbook.step3": "Načti poslední support bundle a recovery joby pro kontrolu výstupu příkazů.",
  "playbook.step4": "Před změnou config nebo auth postury zkontroluj diagnostiku.",
  "playbook.reference": "Reference",
  "summary.title": "Poslední recovery souhrn",
  "summary.description": "Zobraz nejnovější publikovaný souhrn doctoru přímo z diagnostiky.",
  "summary.emptyTitle": "Žádný recovery souhrn nebyl publikován",
  "summary.emptyDescription": "Zařaď doctor preview, aby se naplnila recovery telemetrie.",
  "bundleJobs.title": "Zařazené bundle joby",
  "bundleJobs.description":
    "Joby support bundle zůstávají po dokončení viditelné, aby operátoři mohli ověřit výstupní cesty a důvody selhání.",
  "table.job": "Job",
  "table.state": "Stav",
  "table.actions": "Akce",
  "table.requested": "vyžádáno {value}",
  "table.select": "Vybrat",
  "bundleJobs.emptyTitle": "Nejsou zařazeny žádné support bundle joby",
  "bundleJobs.emptyDescription":
    "Zařaď support bundle a zkontroluj výstup příkazu i cesty k artefaktům.",
  "selectedBundle.title": "Vybraný bundle job",
  "selectedBundle.description":
    "Načti výstup příkazu, výstupní cestu a detail selhání pro zvolený support bundle job.",
  "selectedBundle.load": "Načíst job",
  "selectedBundle.loading": "Načítám...",
  "selectedBundle.jobId": "ID jobu",
  "selectedBundle.emptyTitle": "Není vybraný žádný support bundle job",
  "selectedBundle.emptyDescription": "Vyber job a načti ho pro kontrolu detailů.",
  "recoveryJobs.title": "Recovery joby",
  "recoveryJobs.description":
    "Frontované běhy doctoru drží historii preview/aplikace/rollbacku viditelnou i po odpojení prohlížeče.",
  "recoveryJobs.emptyTitle": "Nejsou zařazeny žádné recovery joby",
  "recoveryJobs.emptyDescription":
    "Zařaď doctor preview nebo zkoušku rollbacku a zkontroluj recovery plán.",
  "selectedRecovery.title": "Vybraný recovery job",
  "selectedRecovery.description":
    "Načti vybraný job doctoru a zkontroluj parsovaný recovery výstup, dostupné rollback běhy a stderr/stdout příkazu.",
  "selectedRecovery.load": "Načíst recovery job",
  "selectedRecovery.jobId": "ID recovery jobu",
  "selectedRecovery.emptyTitle": "Není vybraný žádný recovery job",
  "selectedRecovery.emptyDescription": "Vyber doctor recovery job a načti ho pro kontrolu detailů.",
  "selectedRecovery.summary": "Recovery souhrn",
};

function translateSupport(
  locale: ConsoleAppState["locale"],
  key: SupportMessageKey,
  variables?: Record<string, string | number>,
): string {
  const template = (locale === "cs" ? SUPPORT_MESSAGES_CS : SUPPORT_MESSAGES)[key];
  const resolved =
    variables === undefined
      ? template
      : template.replaceAll(/\{([a-zA-Z0-9_]+)\}/g, (_, name) => `${variables[name] ?? ""}`);
  return locale === "qps-ploc" ? pseudoLocalizeText(resolved) : resolved;
}

export function SupportSection({ app }: SupportSectionProps) {
  const t = (key: SupportMessageKey, variables?: Record<string, string | number>) =>
    translateSupport(app.locale, key, variables);
  const deployment = app.supportDeployment ?? {};
  const warnings = toStringArray(Array.isArray(deployment.warnings) ? deployment.warnings : []);
  const observability = readObject(app.supportDiagnosticsSnapshot ?? {}, "observability");
  const supportBundle = readObject(observability ?? {}, "support_bundle");
  const doctorRecovery = readObject(observability ?? {}, "doctor_recovery");
  const providerAuth = readObject(observability ?? {}, "provider_auth");
  const configRefHealth = readObject(observability ?? {}, "config_ref_health");
  const configRefSummary = readObject(configRefHealth ?? {}, "summary");
  const configRefRecommendations = toStringArray(
    Array.isArray(configRefHealth?.recommendations) ? configRefHealth.recommendations : [],
  );
  const configRefLatestPlan = readObject(configRefHealth ?? {}, "latest_plan");
  const recentFailures = toJsonObjectArray(observability?.recent_failures);
  const latestFailure = recentFailures[0] ?? null;
  const failedJobs = app.supportBundleJobs.filter((job) => readString(job, "state") === "failed");
  const failedDoctorJobs = app.supportDoctorJobs.filter(
    (job) => readString(job, "state") === "failed",
  );
  const providerAuthState = readString(providerAuth ?? {}, "state") ?? "unknown";
  const recoveryBacklog = readNumber(providerAuth ?? {}, "degraded_profiles") ?? 0;
  const configRefState = readString(configRefHealth ?? {}, "state") ?? "unknown";
  const latestDoctorRecovery = readObject(doctorRecovery ?? {}, "last_job");
  const latestDoctorRecoveryState = readString(latestDoctorRecovery ?? {}, "state") ?? "unknown";
  const selectedDoctorReport = readObject(app.supportSelectedDoctorJob ?? {}, "report");
  const selectedDoctorRecovery = readObject(selectedDoctorReport ?? {}, "recovery");
  const selectedDoctorPlannedSteps = toJsonObjectArray(selectedDoctorRecovery?.planned_steps);
  const selectedDoctorAppliedSteps = toJsonObjectArray(selectedDoctorRecovery?.applied_steps);
  const selectedDoctorNextSteps = toStringArray(
    Array.isArray(selectedDoctorRecovery?.next_steps) ? selectedDoctorRecovery.next_steps : [],
  );

  const supportJobRows: SupportJobRow[] = app.supportBundleJobs.map((job) => ({
    jobId: readString(job, "job_id") ?? "unknown",
    state: readString(job, "state") ?? "unknown",
    requestedAt: formatUnixMs(readUnixMillis(job, "requested_at_unix_ms")),
  }));
  const recoveryJobRows: SupportJobRow[] = app.supportDoctorJobs.map((job) => ({
    jobId: readString(job, "job_id") ?? "unknown",
    state: readString(job, "state") ?? "unknown",
    requestedAt: formatUnixMs(readUnixMillis(job, "requested_at_unix_ms")),
  }));

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title={t("header.title")}
        headingLabel={t("header.heading")}
        description={t("header.description")}
        status={
          <>
            <WorkspaceStatusChip tone={failedJobs.length > 0 ? "warning" : "success"}>
              {failedJobs.length} {t("status.failedBundleJobs")}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={failedDoctorJobs.length > 0 ? "warning" : "default"}>
              {failedDoctorJobs.length} {t("status.failedRecoveryJobs")}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={warnings.length > 0 ? "warning" : "default"}>
              {warnings.length} {t("status.deploymentWarnings")}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={latestFailure === null ? "default" : "warning"}>
              {latestFailure === null
                ? t("status.noRecentFailure")
                : t("status.recentFailurePublished")}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            variant="secondary"
            onPress={() => void app.refreshSupport()}
            isDisabled={app.supportBusy}
          >
            {app.supportBusy ? t("action.refreshing") : t("action.refresh")}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          label={t("metric.supportQueue")}
          value={app.supportBundleJobs.length}
          detail={
            app.supportBundleJobs[0] === undefined
              ? t("metric.noQueuedJobs")
              : (readString(app.supportBundleJobs[0], "state") ?? "unknown")
          }
          tone={failedJobs.length > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label={t("metric.recoveryQueue")}
          value={app.supportDoctorJobs.length}
          detail={
            latestDoctorRecovery === null
              ? t("metric.noRecoveryJobs")
              : `${latestDoctorRecoveryState} · ${readString(latestDoctorRecovery, "mode") ?? t("metric.modeUnavailable")}`
          }
          tone={failedDoctorJobs.length > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label={t("metric.bundleReliability")}
          value={formatRate(readNumber(supportBundle ?? {}, "success_rate_bps"))}
          detail={t("metric.attempts", {
            count: readString(supportBundle ?? {}, "attempts") ?? "0",
          })}
          tone={failedJobs.length > 0 ? "warning" : "success"}
        />
        <WorkspaceMetricCard
          label={t("metric.deploymentPosture")}
          value={readString(deployment, "bind_profile") ?? "unknown"}
          detail={readString(deployment, "mode") ?? t("metric.modeUnavailableLong")}
        />
        <WorkspaceMetricCard
          label="Config ref health"
          value={readNumber(configRefSummary ?? {}, "blocking_refs") ?? 0}
          detail={`${readNumber(configRefSummary ?? {}, "warning_refs") ?? 0} warnings Â· ${configRefState}`}
          tone={configRefState === "ok" ? "success" : "warning"}
        />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard title={t("bundle.title")} description={t("bundle.description")}>
          <div className="workspace-stack">
            <TextInputField
              label={t("bundle.retainJobs")}
              value={app.supportBundleRetainJobs}
              onChange={app.setSupportBundleRetainJobs}
            />
            <ActionCluster>
              <ActionButton
                onPress={() => void app.createSupportBundle()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? t("bundle.queueing") : t("bundle.queue")}
              </ActionButton>
              <ActionButton variant="secondary" onPress={() => app.setSection("operations")}>
                {t("bundle.openDiagnostics")}
              </ActionButton>
              <ActionButton variant="secondary" onPress={() => app.setSection("config")}>
                {t("bundle.openConfig")}
              </ActionButton>
            </ActionCluster>
            {warnings.length > 0 ? (
              <InlineNotice title={t("bundle.currentWarnings")} tone="warning">
                <ul className="console-compact-list">
                  {warnings.map((warning) => (
                    <li key={warning}>{warning}</li>
                  ))}
                </ul>
              </InlineNotice>
            ) : null}
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title={t("recoveryPlanner.title")}
          description={t("recoveryPlanner.description")}
        >
          <div className="workspace-stack">
            <div className="workspace-form-grid">
              <TextInputField
                label={t("recoveryPlanner.retainJobs")}
                value={app.supportDoctorRetainJobs}
                onChange={app.setSupportDoctorRetainJobs}
              />
              <TextInputField
                label={t("recoveryPlanner.rollbackRunId")}
                value={app.supportDoctorRollbackRunId}
                onChange={app.setSupportDoctorRollbackRunId}
                description={t("recoveryPlanner.rollbackRunDescription")}
              />
              <TextInputField
                label={t("recoveryPlanner.onlyChecks")}
                value={app.supportDoctorOnly}
                onChange={app.setSupportDoctorOnly}
                description={t("recoveryPlanner.checkDescription")}
              />
              <TextInputField
                label={t("recoveryPlanner.skipChecks")}
                value={app.supportDoctorSkip}
                onChange={app.setSupportDoctorSkip}
                description={t("recoveryPlanner.checkDescription")}
              />
            </div>
            <CheckboxField
              label={t("recoveryPlanner.force")}
              description={t("recoveryPlanner.forceDescription")}
              checked={app.supportDoctorForce}
              onChange={app.setSupportDoctorForce}
            />
            <ActionCluster>
              <ActionButton
                onPress={() => void app.queueDoctorRecoveryPreview()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? t("bundle.queueing") : t("recoveryPlanner.queuePreview")}
              </ActionButton>
              <ActionButton
                variant="secondary"
                onPress={() => void app.queueDoctorRecoveryApply()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? t("bundle.queueing") : t("recoveryPlanner.applyRepairs")}
              </ActionButton>
              <ActionButton
                variant="secondary"
                onPress={() => void app.queueDoctorRollbackPreview()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? t("bundle.queueing") : t("recoveryPlanner.previewRollback")}
              </ActionButton>
              <ActionButton
                variant="secondary"
                onPress={() => void app.queueDoctorRollbackApply()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? t("bundle.queueing") : t("recoveryPlanner.applyRollback")}
              </ActionButton>
            </ActionCluster>
            {latestDoctorRecovery === null ? null : (
              <InlineNotice title={t("recoveryPlanner.latestJob")} tone="default">
                {readString(latestDoctorRecovery, "mode") ?? "unknown mode"} ·{" "}
                {latestDoctorRecoveryState} · planned{" "}
                {readNumber(latestDoctorRecovery, "planned_step_count") ?? 0} / applied{" "}
                {readNumber(latestDoctorRecovery, "applied_step_count") ?? 0}
              </InlineNotice>
            )}
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard title={t("signals.title")} description={t("signals.description")}>
          {latestFailure === null ? (
            <EmptyState
              compact
              title={t("signals.emptyTitle")}
              description={t("signals.emptyDescription")}
            />
          ) : (
            <div className="workspace-stack">
              <InlineNotice
                title={readString(latestFailure, "failure_class") ?? t("signals.unknownFailure")}
                tone="danger"
              >
                {readString(latestFailure, "operation") ?? t("signals.operationUnavailable")} ·{" "}
                {readString(latestFailure, "message_redacted") ??
                  readString(latestFailure, "message") ??
                  t("signals.noRedactedMessage")}
              </InlineNotice>
              <PrettyJsonBlock
                value={latestFailure}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            </div>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard title={t("provider.title")} description={t("provider.description")}>
          <div className="workspace-stack">
            <div className="workspace-inline">
              <WorkspaceStatusChip
                tone={
                  providerAuthState === "missing" || providerAuthState === "expired"
                    ? "danger"
                    : providerAuthState === "degraded"
                      ? "warning"
                      : "success"
                }
              >
                {providerAuthState}
              </WorkspaceStatusChip>
              <WorkspaceStatusChip tone={recoveryBacklog > 0 ? "warning" : "default"}>
                {recoveryBacklog} {t("provider.degradedProfiles")}
              </WorkspaceStatusChip>
            </div>
            <p className="chat-muted">{t("provider.body")}</p>
            <ActionCluster>
              <ActionButton variant="secondary" onPress={() => app.setSection("operations")}>
                {t("provider.openDiagnostics")}
              </ActionButton>
              <ActionButton variant="secondary" onPress={() => app.setSection("auth")}>
                {t("provider.openAuthProfiles")}
              </ActionButton>
            </ActionCluster>
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-stack">
        <WorkspaceSectionCard
          title="Secret and reload health"
          description="Keep configured secret-source health, reload blockers, and operator advice close to support actions."
        >
          {configRefHealth === null ? (
            <EmptyState
              compact
              title="No secret or reload health published"
              description="Refresh support after the daemon publishes configured ref health."
            />
          ) : (
            <div className="workspace-stack">
              <div className="workspace-inline">
                <WorkspaceStatusChip tone={configRefState === "ok" ? "success" : "warning"}>
                  {configRefState}
                </WorkspaceStatusChip>
                <WorkspaceStatusChip
                  tone={
                    (readNumber(configRefSummary ?? {}, "blocking_refs") ?? 0) > 0
                      ? "warning"
                      : "default"
                  }
                >
                  {readNumber(configRefSummary ?? {}, "blocking_refs") ?? 0} blocking refs
                </WorkspaceStatusChip>
                <WorkspaceStatusChip
                  tone={
                    (readNumber(configRefSummary ?? {}, "blocked_while_runs_active_refs") ?? 0) > 0
                      ? "warning"
                      : "default"
                  }
                >
                  {readNumber(configRefSummary ?? {}, "blocked_while_runs_active_refs") ?? 0} reload
                  blockers
                </WorkspaceStatusChip>
              </div>
              {configRefRecommendations.length > 0 ? (
                <InlineNotice title="Recommended next steps" tone="warning">
                  <ul className="console-compact-list">
                    {configRefRecommendations.slice(0, 4).map((recommendation) => (
                      <li key={recommendation}>{recommendation}</li>
                    ))}
                  </ul>
                </InlineNotice>
              ) : null}
              {configRefLatestPlan === null ? null : (
                <PrettyJsonBlock
                  value={configRefLatestPlan}
                  revealSensitiveValues={app.revealSensitiveValues}
                />
              )}
            </div>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard title={t("playbook.title")} description={t("playbook.description")}>
          <div className="workspace-stack">
            <ol className="workspace-bullet-list">
              <li>{t("playbook.step1")}</li>
              <li>{t("playbook.step2")}</li>
              <li>{t("playbook.step3")}</li>
              <li>{t("playbook.step4")}</li>
            </ol>
            <InlineNotice title={t("playbook.reference")} tone="default">
              docs-codebase/docs-tree/web_console_operator_dashboard/console_sections_and_navigation/support_recovery.md
            </InlineNotice>
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard title={t("summary.title")} description={t("summary.description")}>
          {latestDoctorRecovery === null ? (
            <EmptyState
              compact
              title={t("summary.emptyTitle")}
              description={t("summary.emptyDescription")}
            />
          ) : (
            <PrettyJsonBlock
              value={latestDoctorRecovery}
              revealSensitiveValues={app.revealSensitiveValues}
            />
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title={t("bundleJobs.title")}
          description={t("bundleJobs.description")}
        >
          <EntityTable
            ariaLabel="Support bundle jobs"
            columns={[
              {
                key: "job",
                label: t("table.job"),
                isRowHeader: true,
                render: (row: SupportJobRow) => (
                  <div className="workspace-stack">
                    <strong>{row.jobId}</strong>
                    <span className="chat-muted">
                      {t("table.requested", { value: row.requestedAt })}
                    </span>
                  </div>
                ),
              },
              {
                key: "state",
                label: t("table.state"),
                render: (row: SupportJobRow) => (
                  <WorkspaceStatusChip tone={row.state === "failed" ? "danger" : "default"}>
                    {row.state}
                  </WorkspaceStatusChip>
                ),
              },
              {
                key: "actions",
                label: t("table.actions"),
                align: "end",
                render: (row: SupportJobRow) => (
                  <ActionButton
                    variant="secondary"
                    size="sm"
                    onPress={() => app.setSupportSelectedBundleJobId(row.jobId)}
                  >
                    {t("table.select")}
                  </ActionButton>
                ),
              },
            ]}
            rows={supportJobRows}
            getRowId={(row) => row.jobId}
            emptyTitle={t("bundleJobs.emptyTitle")}
            emptyDescription={t("bundleJobs.emptyDescription")}
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title={t("selectedBundle.title")}
          description={t("selectedBundle.description")}
          actions={
            <ActionButton
              variant="secondary"
              size="sm"
              onPress={() => void app.loadSupportBundleJob()}
              isDisabled={app.supportBusy}
            >
              {app.supportBusy ? t("selectedBundle.loading") : t("selectedBundle.load")}
            </ActionButton>
          }
        >
          <div className="workspace-stack">
            <TextInputField
              label={t("selectedBundle.jobId")}
              value={app.supportSelectedBundleJobId}
              onChange={app.setSupportSelectedBundleJobId}
            />

            {app.supportSelectedBundleJob === null ? (
              <EmptyState
                compact
                title={t("selectedBundle.emptyTitle")}
                description={t("selectedBundle.emptyDescription")}
              />
            ) : (
              <PrettyJsonBlock
                value={app.supportSelectedBundleJob}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title={t("recoveryJobs.title")}
          description={t("recoveryJobs.description")}
        >
          <EntityTable
            ariaLabel="Doctor recovery jobs"
            columns={[
              {
                key: "job",
                label: t("table.job"),
                isRowHeader: true,
                render: (row: SupportJobRow) => (
                  <div className="workspace-stack">
                    <strong>{row.jobId}</strong>
                    <span className="chat-muted">
                      {t("table.requested", { value: row.requestedAt })}
                    </span>
                  </div>
                ),
              },
              {
                key: "state",
                label: t("table.state"),
                render: (row: SupportJobRow) => (
                  <WorkspaceStatusChip tone={row.state === "failed" ? "danger" : "default"}>
                    {row.state}
                  </WorkspaceStatusChip>
                ),
              },
              {
                key: "actions",
                label: t("table.actions"),
                align: "end",
                render: (row: SupportJobRow) => (
                  <ActionButton
                    variant="secondary"
                    size="sm"
                    onPress={() => app.setSupportSelectedDoctorJobId(row.jobId)}
                  >
                    {t("table.select")}
                  </ActionButton>
                ),
              },
            ]}
            rows={recoveryJobRows}
            getRowId={(row) => row.jobId}
            emptyTitle={t("recoveryJobs.emptyTitle")}
            emptyDescription={t("recoveryJobs.emptyDescription")}
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title={t("selectedRecovery.title")}
          description={t("selectedRecovery.description")}
          actions={
            <ActionButton
              variant="secondary"
              size="sm"
              onPress={() => void app.loadDoctorRecoveryJob()}
              isDisabled={app.supportBusy}
            >
              {app.supportBusy ? t("selectedBundle.loading") : t("selectedRecovery.load")}
            </ActionButton>
          }
        >
          <div className="workspace-stack">
            <TextInputField
              label={t("selectedRecovery.jobId")}
              value={app.supportSelectedDoctorJobId}
              onChange={app.setSupportSelectedDoctorJobId}
            />

            {app.supportSelectedDoctorJob === null ? (
              <EmptyState
                compact
                title={t("selectedRecovery.emptyTitle")}
                description={t("selectedRecovery.emptyDescription")}
              />
            ) : (
              <>
                {selectedDoctorRecovery === null ? null : (
                  <div className="workspace-stack">
                    <InlineNotice
                      title={
                        readString(selectedDoctorReport ?? {}, "mode") ??
                        t("selectedRecovery.summary")
                      }
                      tone={
                        readString(app.supportSelectedDoctorJob, "state") === "failed"
                          ? "danger"
                          : "default"
                      }
                    >
                      run {readString(selectedDoctorRecovery, "run_id") ?? "preview-only"} · planned{" "}
                      {selectedDoctorPlannedSteps.length} · applied{" "}
                      {selectedDoctorAppliedSteps.length}
                    </InlineNotice>
                    {selectedDoctorPlannedSteps.length > 0 ? (
                      <div className="workspace-stack">
                        <strong>Planned steps</strong>
                        <ul className="console-compact-list">
                          {selectedDoctorPlannedSteps.map((step, index) => (
                            <li key={`${readString(step, "id") ?? "planned"}-${index}`}>
                              {readString(step, "title") ??
                                readString(step, "id") ??
                                "Unnamed step"}
                            </li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                    {selectedDoctorAppliedSteps.length > 0 ? (
                      <div className="workspace-stack">
                        <strong>Applied steps</strong>
                        <ul className="console-compact-list">
                          {selectedDoctorAppliedSteps.map((step, index) => (
                            <li key={`${readString(step, "id") ?? "applied"}-${index}`}>
                              {readString(step, "message") ??
                                readString(step, "id") ??
                                "Unnamed step"}
                            </li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                    {selectedDoctorNextSteps.length > 0 ? (
                      <div className="workspace-stack">
                        <strong>Next steps</strong>
                        <ul className="console-compact-list">
                          {selectedDoctorNextSteps.map((step) => (
                            <li key={step}>{step}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                  </div>
                )}
                <PrettyJsonBlock
                  value={app.supportSelectedDoctorJob}
                  revealSensitiveValues={app.revealSensitiveValues}
                />
              </>
            )}
          </div>
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function readUnixMillis(record: JsonObject, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatRate(value: number | null): string {
  if (value === null) {
    return "n/a";
  }
  return `${(value / 100).toFixed(2)}%`;
}

function toJsonObjectArray(value: unknown): JsonObject[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry): entry is JsonObject => {
    return entry !== null && typeof entry === "object" && !Array.isArray(entry);
  });
}
