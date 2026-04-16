import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  buildSessionLineageHint,
  describeBranchState,
  describeTitleGenerationState,
} from "../../chat/chatShared";
import type {
  ChatCheckpointRecord,
  ChatCompactionArtifactRecord,
  ChatCompactionPreview,
} from "../../consoleApi";
import {
  buildObjectiveChatHref,
  buildObjectiveOverviewHref,
  findObjectiveForSession,
} from "../objectiveLinks";
import { getSectionPath } from "../navigation";
import { ActionButton, SelectField, SwitchField, TextInputField } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import { useSessionCatalogDomain } from "../hooks/useSessionCatalogDomain";
import { pseudoLocalizeText } from "../i18n";
import { formatUnixMs, isJsonObject, readString, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";
import { buildSessionCatalogPresentation } from "./sessionCatalogPresentation";

type SessionsSectionProps = {
  app: Pick<ConsoleAppState, "api" | "locale" | "setError" | "setNotice">;
};

const SESSION_MESSAGES = {
  "header.title": "Sessions",
  "header.description":
    "Search session history, inspect latest run posture, and drive lifecycle actions without leaving the operator console.",
  "status.refreshing": "Refreshing",
  "status.catalogReady": "Catalog ready",
  "status.pendingApprovals": "pending approvals",
  "status.noRunSelected": "No run selected",
  "action.refresh": "Refresh sessions",
  "action.refreshing": "Refreshing...",
  "action.rename": "Rename",
  "action.reset": "Reset",
  "action.archive": "Archive",
  "action.abortRun": "Abort run",
  "action.createCheckpoint": "Create checkpoint",
  "action.checkpointing": "Checkpointing...",
  "action.previewCompaction": "Preview compaction",
  "action.previewing": "Previewing...",
  "action.applyCompaction": "Apply compaction",
  "action.applying": "Applying...",
  "action.openChat": "Open in chat",
  "action.openObjective": "Open objective",
  "action.openInventory": "Open inventory",
  "metric.activeSessions": "Active sessions",
  "metric.activeSessionsDetail": "Visible non-archived sessions in the current scoped catalog.",
  "metric.archivedSessions": "Archived sessions",
  "metric.archivedSessionsDetail":
    "Archived records stay queryable without reopening the chat rail.",
  "metric.pendingApprovals": "Pending approvals",
  "metric.pendingApprovalsDetail": "Sessions currently waiting on sensitive-action decisions.",
  "metric.activeRuns": "Active runs",
  "metric.activeRunsDetail": "Latest known run is still accepted or in progress.",
  "metric.contextFiles": "Context files",
  "metric.contextFilesDetail": "Sessions carrying active context files or workspace references.",
  "filters.title": "Filters",
  "filters.description":
    "Catalog filters stay server-backed so chat, web, and future operator surfaces do not invent separate session logic.",
  "filters.search": "Search",
  "filters.searchPlaceholder": "title, family, agent, model, file, or recap",
  "filters.sort": "Sort",
  "filters.sort.updatedDesc": "Updated (newest)",
  "filters.sort.updatedAsc": "Updated (oldest)",
  "filters.sort.createdDesc": "Created (newest)",
  "filters.sort.createdAsc": "Created (oldest)",
  "filters.sort.titleAsc": "Title (A-Z)",
  "filters.titleMode": "Title mode",
  "filters.titleMode.all": "Any title mode",
  "filters.titleMode.ready": "Auto title ready",
  "filters.titleMode.pending": "Auto title pending",
  "filters.titleMode.failed": "Auto title failed",
  "filters.titleMode.idle": "Auto title idle",
  "filters.titleSource": "Title source",
  "filters.titleSource.all": "Any title source",
  "filters.titleSource.label": "Manual label",
  "filters.titleSource.semantic": "Semantic title",
  "filters.titleSource.auto": "Automatic title",
  "filters.titleSource.sessionKey": "Session key fallback",
  "filters.branchState": "Branch state",
  "filters.branchState.all": "Any lineage",
  "filters.branchState.root": "Root session",
  "filters.branchState.active": "Active branch",
  "filters.branchState.source": "Branch source",
  "filters.pendingApprovals": "Pending approvals",
  "filters.pendingApprovals.all": "Any approval state",
  "filters.pendingApprovals.yes": "With pending approvals",
  "filters.pendingApprovals.no": "Without pending approvals",
  "filters.contextFiles": "Context files",
  "filters.contextFiles.all": "Any context posture",
  "filters.contextFiles.yes": "With context files",
  "filters.contextFiles.no": "Without context files",
  "filters.agent": "Agent",
  "filters.agentPlaceholder": "agent id",
  "filters.modelProfile": "Model profile",
  "filters.modelProfilePlaceholder": "model profile",
  "filters.showArchived": "Show archived",
  "filters.showArchivedDescription": "Include archived records in the current list.",
  "catalog.title": "Catalog",
  "catalog.description":
    "Pick a session to inspect its latest activity, preview, and lifecycle state.",
  "catalog.emptyTitle": "No sessions match the current query",
  "catalog.emptyDescription":
    "Adjust filters or create activity in chat to populate the session catalog.",
  "catalog.columns.title": "Title",
  "catalog.columns.family": "Family",
  "catalog.columns.updated": "Updated",
  "catalog.columns.controls": "Controls",
  "catalog.columns.recap": "Recap",
  "catalog.archived": "archived",
  "catalog.noRecap": "No recap",
  "detail.title": "Detail",
  "detail.description":
    "Lifecycle actions here reuse the same backend mutations as chat instead of inventing a separate control path.",
  "detail.emptyTitle": "No session selected",
  "detail.emptyDescription":
    "Select a row from the session catalog to inspect details and actions.",
  "detail.selectedSession": "Selected session",
  "detail.noPreview": "No preview was derivable from existing run history.",
  "detail.family": "Family {index}/{count}",
  "detail.sessionLabel": "Session label",
  "detail.sessionLabelDescription": "Leave empty to return the session to automatic title mode.",
  "detail.sessionKey": "Session key",
  "detail.titleSource": "Title source",
  "detail.familyRoot": "Family root",
  "detail.created": "Created",
  "detail.updated": "Updated",
  "detail.runState": "Run state",
  "detail.lineage": "Lineage",
  "detail.totalTokens": "Total tokens",
  "detail.contextFiles": "Context files",
  "detail.active": "{count} active",
  "detail.none": "none",
  "detail.latestActivity": "Latest activity",
  "detail.lastIntent": "Last intent:",
  "detail.lastSummary": "Last summary:",
  "detail.missing": "Missing",
  "detail.resumeRecap": "Resume recap",
  "detail.touchedFiles": "Touched files:",
  "detail.activeContext": "Active context:",
  "detail.recentArtifacts": "Recent artifacts:",
  "detail.objectiveLinkage": "Objective linkage",
  "detail.linkedObjective": "Linked objective",
  "detail.loadingObjectiveLinkage": "Loading objective linkage for the selected session.",
  "detail.noObjective": "No objective currently points at this session.",
  "detail.unnamedObjective": "Unnamed objective",
  "detail.currentFocus": "Current focus:",
  "detail.noCurrentFocus": "No current focus recorded.",
  "detail.nextAction": "Next action:",
  "detail.noNextAction": "No next action recorded.",
  "continuity.preview": "Compaction preview",
  "continuity.blocked": "Compaction blocked",
  "continuity.summary": "Summary:",
  "continuity.tokenDelta": "Token delta:",
  "continuity.plannedWrites": "Planned writes:",
  "continuity.reviewCandidates": "Review candidates:",
  "continuity.reviewHelp":
    "Use the chat compaction flow to accept or reject the review-required candidates explicitly.",
  "continuity.loading": "Loading continuity",
  "continuity.compactions": "{count} compactions",
  "continuity.checkpoints": "{count} checkpoints",
  "continuity.pendingReview": "{count} pending review",
  "continuity.artifacts": "Continuity artifacts",
  "continuity.recentCompactions": "Recent compactions",
  "continuity.recentCompactionsDescription":
    "Inspect the last stored compactions and jump straight into the chat detail sidebar for raw diff and audit context.",
  "continuity.emptyCompactionsTitle": "No compactions yet",
  "continuity.emptyCompactionsDescription":
    "No compaction artifacts are stored for this session yet.",
  "continuity.review": "review",
  "continuity.recoveryPoints": "Recovery points",
  "continuity.recentCheckpoints": "Recent checkpoints",
  "continuity.recentCheckpointsDescription":
    "Checkpoints stay paired with compaction history so rollback is visible without opening the raw journal.",
  "continuity.emptyCheckpointsTitle": "No checkpoints yet",
  "continuity.emptyCheckpointsDescription":
    "Create a checkpoint or apply a compaction to start the rollback history.",
  "continuity.restores": "restores",
  "continuity.noCheckpointNote": "No note recorded for this checkpoint.",
} as const;

type SessionMessageKey = keyof typeof SESSION_MESSAGES;

const SESSION_MESSAGES_CS: Readonly<Record<SessionMessageKey, string>> = {
  "header.title": "Relace",
  "header.description":
    "Procházej historii relací, kontroluj poslední stav běhu a spouštěj lifecycle akce bez opuštění operátorské konzole.",
  "status.refreshing": "Obnovuji",
  "status.catalogReady": "Katalog připraven",
  "status.pendingApprovals": "čekajících schválení",
  "status.noRunSelected": "Není vybraný běh",
  "action.refresh": "Obnovit relace",
  "action.refreshing": "Obnovuji...",
  "action.rename": "Přejmenovat",
  "action.reset": "Resetovat",
  "action.archive": "Archivovat",
  "action.abortRun": "Přerušit běh",
  "action.createCheckpoint": "Vytvořit checkpoint",
  "action.checkpointing": "Vytvářím checkpoint...",
  "action.previewCompaction": "Preview kompakce",
  "action.previewing": "Připravuji preview...",
  "action.applyCompaction": "Aplikovat kompakci",
  "action.applying": "Aplikuji...",
  "action.openChat": "Otevřít v chatu",
  "action.openObjective": "Otevřít objective",
  "action.openInventory": "Otevřít inventář",
  "metric.activeSessions": "Aktivní relace",
  "metric.activeSessionsDetail": "Viditelné nearhivované relace v aktuálním scoped katalogu.",
  "metric.archivedSessions": "Archivované relace",
  "metric.archivedSessionsDetail":
    "Archivované záznamy zůstávají dotazovatelné bez znovuotevření chat railu.",
  "metric.pendingApprovals": "Čekající schválení",
  "metric.pendingApprovalsDetail": "Relace, které právě čekají na rozhodnutí o citlivé akci.",
  "metric.activeRuns": "Aktivní běhy",
  "metric.activeRunsDetail": "Poslední známý běh je stále přijatý nebo probíhá.",
  "metric.contextFiles": "Kontextové soubory",
  "metric.contextFilesDetail":
    "Relace nesoucí aktivní kontextové soubory nebo odkazy na workspace.",
  "filters.title": "Filtry",
  "filters.description":
    "Filtry katalogu zůstávají server-backed, takže chat, web a budoucí operátorské surface nevymýšlejí oddělenou logiku relací.",
  "filters.search": "Hledat",
  "filters.searchPlaceholder": "název, rodina, agent, model, soubor nebo recap",
  "filters.sort": "Řazení",
  "filters.sort.updatedDesc": "Aktualizováno (nejnovější)",
  "filters.sort.updatedAsc": "Aktualizováno (nejstarší)",
  "filters.sort.createdDesc": "Vytvořeno (nejnovější)",
  "filters.sort.createdAsc": "Vytvořeno (nejstarší)",
  "filters.sort.titleAsc": "Název (A-Z)",
  "filters.titleMode": "Režim názvu",
  "filters.titleMode.all": "Libovolný režim názvu",
  "filters.titleMode.ready": "Auto název připraven",
  "filters.titleMode.pending": "Auto název čeká",
  "filters.titleMode.failed": "Auto název selhal",
  "filters.titleMode.idle": "Auto název nečinný",
  "filters.titleSource": "Zdroj názvu",
  "filters.titleSource.all": "Libovolný zdroj názvu",
  "filters.titleSource.label": "Ruční štítek",
  "filters.titleSource.semantic": "Sémantický název",
  "filters.titleSource.auto": "Automatický název",
  "filters.titleSource.sessionKey": "Fallback session key",
  "filters.branchState": "Stav větve",
  "filters.branchState.all": "Libovolná lineage",
  "filters.branchState.root": "Kořenová relace",
  "filters.branchState.active": "Aktivní větev",
  "filters.branchState.source": "Zdroj větve",
  "filters.pendingApprovals": "Čekající schválení",
  "filters.pendingApprovals.all": "Libovolný stav schválení",
  "filters.pendingApprovals.yes": "S čekajícími schváleními",
  "filters.pendingApprovals.no": "Bez čekajících schválení",
  "filters.contextFiles": "Kontextové soubory",
  "filters.contextFiles.all": "Libovolná kontextová postura",
  "filters.contextFiles.yes": "S kontextovými soubory",
  "filters.contextFiles.no": "Bez kontextových souborů",
  "filters.agent": "Agent",
  "filters.agentPlaceholder": "id agenta",
  "filters.modelProfile": "Profil modelu",
  "filters.modelProfilePlaceholder": "profil modelu",
  "filters.showArchived": "Zobrazit archivované",
  "filters.showArchivedDescription": "Zahrnout archivované záznamy do aktuálního seznamu.",
  "catalog.title": "Katalog",
  "catalog.description":
    "Vyber relaci a zkontroluj její poslední aktivitu, preview a lifecycle stav.",
  "catalog.emptyTitle": "Aktuálnímu dotazu neodpovídají žádné relace",
  "catalog.emptyDescription":
    "Uprav filtry nebo vytvoř aktivitu v chatu, aby se katalog relací naplnil.",
  "catalog.columns.title": "Název",
  "catalog.columns.family": "Rodina",
  "catalog.columns.updated": "Aktualizováno",
  "catalog.columns.controls": "Ovládání",
  "catalog.columns.recap": "Recap",
  "catalog.archived": "archivováno",
  "catalog.noRecap": "Žádný recap",
  "detail.title": "Detail",
  "detail.description":
    "Lifecycle akce tady znovu používají stejné backendové mutace jako chat místo vymýšlení separátní control path.",
  "detail.emptyTitle": "Není vybraná žádná relace",
  "detail.emptyDescription": "Vyber řádek z katalogu relací a zkontroluj detaily i akce.",
  "detail.selectedSession": "Vybraná relace",
  "detail.noPreview": "Ze stávající historie běhů nešlo odvodit žádné preview.",
  "detail.family": "Rodina {index}/{count}",
  "detail.sessionLabel": "Štítek relace",
  "detail.sessionLabelDescription":
    "Ponech prázdné, pokud chceš relaci vrátit do automatického režimu názvu.",
  "detail.sessionKey": "Session key",
  "detail.titleSource": "Zdroj názvu",
  "detail.familyRoot": "Kořen rodiny",
  "detail.created": "Vytvořeno",
  "detail.updated": "Aktualizováno",
  "detail.runState": "Stav běhu",
  "detail.lineage": "Lineage",
  "detail.totalTokens": "Celkem tokenů",
  "detail.contextFiles": "Kontextové soubory",
  "detail.active": "{count} aktivních",
  "detail.none": "žádné",
  "detail.latestActivity": "Poslední aktivita",
  "detail.lastIntent": "Poslední intent:",
  "detail.lastSummary": "Poslední souhrn:",
  "detail.missing": "Chybí",
  "detail.resumeRecap": "Resume recap",
  "detail.touchedFiles": "Dotčené soubory:",
  "detail.activeContext": "Aktivní kontext:",
  "detail.recentArtifacts": "Nedávné artefakty:",
  "detail.objectiveLinkage": "Vazba na objective",
  "detail.linkedObjective": "Navázaný objective",
  "detail.loadingObjectiveLinkage": "Načítám vazbu objective pro vybranou relaci.",
  "detail.noObjective": "Na tuto relaci momentálně neukazuje žádný objective.",
  "detail.unnamedObjective": "Objective bez názvu",
  "detail.currentFocus": "Aktuální fokus:",
  "detail.noCurrentFocus": "Není zaznamenaný žádný aktuální fokus.",
  "detail.nextAction": "Další akce:",
  "detail.noNextAction": "Není zaznamenaná žádná další akce.",
  "continuity.preview": "Preview kompakce",
  "continuity.blocked": "Kompakce zablokována",
  "continuity.summary": "Souhrn:",
  "continuity.tokenDelta": "Rozdíl tokenů:",
  "continuity.plannedWrites": "Plánované zápisy:",
  "continuity.reviewCandidates": "Kandidáti k review:",
  "continuity.reviewHelp":
    "Použij compaction flow v chatu a kandidáty vyžadující review explicitně přijmi nebo zamítni.",
  "continuity.loading": "Načítám kontinuitu",
  "continuity.compactions": "{count} kompakcí",
  "continuity.checkpoints": "{count} checkpointů",
  "continuity.pendingReview": "{count} čeká na review",
  "continuity.artifacts": "Artefakty kontinuity",
  "continuity.recentCompactions": "Nedávné kompakce",
  "continuity.recentCompactionsDescription":
    "Zkontroluj poslední uložené kompakce a skoč rovnou do chat detail sidebaru pro raw diff a auditní kontext.",
  "continuity.emptyCompactionsTitle": "Zatím žádné kompakce",
  "continuity.emptyCompactionsDescription":
    "Pro tuto relaci zatím nejsou uložené žádné artefakty kompakce.",
  "continuity.review": "review",
  "continuity.recoveryPoints": "Body obnovy",
  "continuity.recentCheckpoints": "Nedávné checkpointy",
  "continuity.recentCheckpointsDescription":
    "Checkpointy zůstávají spárované s historií kompakce, takže rollback je viditelný i bez otevření raw journalu.",
  "continuity.emptyCheckpointsTitle": "Zatím žádné checkpointy",
  "continuity.emptyCheckpointsDescription":
    "Vytvoř checkpoint nebo aplikuj kompakci a tím založ rollback historii.",
  "continuity.restores": "obnovení",
  "continuity.noCheckpointNote": "Pro tento checkpoint není zaznamenaná žádná poznámka.",
};

function translateSession(
  locale: ConsoleAppState["locale"],
  key: SessionMessageKey,
  variables?: Record<string, string | number>,
): string {
  const template = (locale === "cs" ? SESSION_MESSAGES_CS : SESSION_MESSAGES)[key];
  const resolved =
    variables === undefined
      ? template
      : template.replaceAll(/\{([a-zA-Z0-9_]+)\}/g, (_, name) => `${variables[name] ?? ""}`);
  return locale === "qps-ploc" ? pseudoLocalizeText(resolved) : resolved;
}

export function SessionsSection({ app }: SessionsSectionProps) {
  const t = (key: SessionMessageKey, variables?: Record<string, string | number>) =>
    translateSession(app.locale, key, variables);
  const continuityCountLabel = (kind: "write", count: number): string => {
    if (kind === "write") {
      if (app.locale === "cs") {
        return `${count} zápis${count === 1 ? "" : count >= 2 && count <= 4 ? "y" : "ů"}`;
      }
      return `${count} write${count === 1 ? "" : "s"}`;
    }
    return String(count);
  };
  const navigate = useNavigate();
  const catalog = useSessionCatalogDomain(app);
  const selected = catalog.selectedSession;
  const [phase4Busy, setPhase4Busy] = useState<"checkpoint" | "preview" | "apply" | null>(null);
  const [continuityBusy, setContinuityBusy] = useState(false);
  const [sessionCompactions, setSessionCompactions] = useState<ChatCompactionArtifactRecord[]>([]);
  const [sessionCheckpoints, setSessionCheckpoints] = useState<ChatCheckpointRecord[]>([]);
  const [compactionPreview, setCompactionPreview] = useState<ChatCompactionPreview | null>(null);
  const [objectivesBusy, setObjectivesBusy] = useState(false);
  const [objectives, setObjectives] = useState<JsonObject[]>([]);
  const selectedLineage = buildSessionLineageHint(selected);
  const selectedPresentation = buildSessionCatalogPresentation(selected);
  const selectedObjective = useMemo(
    () =>
      findObjectiveForSession(
        objectives,
        selected === null
          ? null
          : {
              session_id: selected.session_id,
              session_key: selected.session_key,
              session_label: selected.session_label,
            },
      ),
    [objectives, selected],
  );

  useEffect(() => {
    let cancelled = false;

    async function loadContinuitySummary(): Promise<void> {
      if (selected === null) {
        setSessionCompactions([]);
        setSessionCheckpoints([]);
        setCompactionPreview(null);
        return;
      }

      setContinuityBusy(true);
      app.setError(null);
      try {
        const response = await app.api.getSessionTranscript(selected.session_id);
        if (cancelled) {
          return;
        }
        setSessionCompactions(response.compactions);
        setSessionCheckpoints(response.checkpoints);
      } catch (error) {
        if (!cancelled) {
          app.setError(error instanceof Error ? error.message : "Unexpected failure.");
        }
      } finally {
        if (!cancelled) {
          setContinuityBusy(false);
        }
      }
    }

    void loadContinuitySummary();
    return () => {
      cancelled = true;
    };
  }, [app, selected?.session_id]);

  useEffect(() => {
    let cancelled = false;

    async function loadObjectives(): Promise<void> {
      setObjectivesBusy(true);
      try {
        const response = await app.api.listObjectives(new URLSearchParams({ limit: "64" }));
        if (cancelled) {
          return;
        }
        setObjectives(
          Array.isArray(response.objectives) ? response.objectives.filter(isJsonObject) : [],
        );
      } catch (error) {
        if (!cancelled) {
          app.setError(error instanceof Error ? error.message : "Unexpected failure.");
        }
      } finally {
        if (!cancelled) {
          setObjectivesBusy(false);
        }
      }
    }

    void loadObjectives();
    return () => {
      cancelled = true;
    };
  }, [app]);

  async function createCheckpoint(): Promise<void> {
    if (selected === null) {
      app.setError(t("detail.emptyTitle"));
      return;
    }
    setPhase4Busy("checkpoint");
    app.setError(null);
    app.setNotice(null);
    try {
      const label = selected.session_label?.trim() || selected.title.trim() || "Session";
      const response = await app.api.createSessionCheckpoint(selected.session_id, {
        name: `${label} checkpoint`,
        note: `Created from the Sessions console on ${new Date().toLocaleString()}.`,
        tags: ["web-console", "sessions-section"],
      });
      setSessionCheckpoints((previous) => [...previous, response.checkpoint]);
      app.setNotice(`${t("action.createCheckpoint")}: ${response.checkpoint.name}.`);
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Unexpected failure.");
    } finally {
      setPhase4Busy(null);
    }
  }

  async function previewCompaction(): Promise<void> {
    if (selected === null) {
      app.setError(t("detail.emptyTitle"));
      return;
    }
    setPhase4Busy("preview");
    app.setError(null);
    app.setNotice(null);
    try {
      const response = await app.api.previewSessionCompaction(selected.session_id, {
        trigger_reason: "sessions_section_preview",
        trigger_policy: "operator_preview",
      });
      setCompactionPreview(response.preview);
      const summary = readCompactionSummary(response.preview.summary);
      const reviewCount = summary?.planner?.review_candidate_count ?? 0;
      const writeCount = summary?.writes?.length ?? 0;
      app.setNotice(
        response.preview.eligible
          ? `${t("continuity.preview")}: ${continuityCountLabel("write", writeCount)}${reviewCount > 0 ? ` · ${reviewCount} ${t("continuity.review")}` : ""}.`
          : t("continuity.blocked"),
      );
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Unexpected failure.");
    } finally {
      setPhase4Busy(null);
    }
  }

  async function applyCompaction(): Promise<void> {
    if (selected === null) {
      app.setError(t("detail.emptyTitle"));
      return;
    }

    const preview =
      compactionPreview?.trigger_reason === "sessions_section_preview"
        ? compactionPreview
        : await app.api
            .previewSessionCompaction(selected.session_id, {
              trigger_reason: "sessions_section_preview",
              trigger_policy: "operator_preview",
            })
            .then((response) => {
              setCompactionPreview(response.preview);
              return response.preview;
            });
    const summary = readCompactionSummary(preview.summary);
    const reviewCount = summary?.planner?.review_candidate_count ?? 0;
    if (reviewCount > 0) {
      app.setNotice(`${reviewCount} ${t("continuity.review")} · ${t("continuity.reviewHelp")}`);
      return;
    }

    setPhase4Busy("apply");
    app.setError(null);
    app.setNotice(null);
    try {
      const response = await app.api.applySessionCompaction(selected.session_id, {
        trigger_reason: "sessions_section_apply",
        trigger_policy: "operator_apply",
      });
      setSessionCompactions((previous) => [...previous, response.artifact]);
      setSessionCheckpoints((previous) => [...previous, response.checkpoint]);
      setCompactionPreview(response.preview);
      const appliedSummary = safeParseCompactionSummaryJson(response.artifact.summary_json);
      const writeCount = appliedSummary?.writes?.length ?? 0;
      app.setNotice(
        `${t("action.applyCompaction")}: ${continuityCountLabel("write", writeCount)} · ${response.checkpoint.name}.`,
      );
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Unexpected failure.");
    } finally {
      setPhase4Busy(null);
    }
  }

  function openChatWithArtifact(options: {
    runId?: string;
    compactionId?: string;
    checkpointId?: string;
  }): void {
    if (selected === null) {
      return;
    }
    const search = new URLSearchParams();
    search.set("sessionId", selected.session_id);
    if (options.runId !== undefined && options.runId.length > 0) {
      search.set("runId", options.runId);
    }
    if (options.compactionId !== undefined && options.compactionId.length > 0) {
      search.set("compactionId", options.compactionId);
    }
    if (options.checkpointId !== undefined && options.checkpointId.length > 0) {
      search.set("checkpointId", options.checkpointId);
    }
    void navigate(`${getSectionPath("chat")}?${search.toString()}`);
  }

  const continuitySummary =
    compactionPreview === null ? null : readCompactionSummary(compactionPreview.summary);
  const continuityReviewCount = continuitySummary?.planner?.review_candidate_count ?? 0;
  const continuityWriteCount = continuitySummary?.writes?.length ?? 0;
  const recentCompactions = [...sessionCompactions].reverse().slice(0, 3);
  const recentCheckpoints = [...sessionCheckpoints].reverse().slice(0, 3);

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title={t("header.title")}
        description={t("header.description")}
        status={
          <>
            <WorkspaceStatusChip tone={catalog.busy ? "warning" : "success"}>
              {catalog.busy ? t("status.refreshing") : t("status.catalogReady")}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={selected?.pending_approvals ? "warning" : "default"}>
              {selected?.pending_approvals ?? 0} {t("status.pendingApprovals")}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={workspaceToneForState(selected?.last_run_state ?? "unknown")}
            >
              {selected?.last_run_state ?? t("status.noRunSelected")}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            isDisabled={catalog.busy}
            type="button"
            variant="primary"
            onPress={() => void catalog.refreshSessions()}
          >
            {catalog.busy ? t("action.refreshing") : t("action.refresh")}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail={t("metric.activeSessionsDetail")}
          label={t("metric.activeSessions")}
          value={catalog.summary?.active_sessions ?? 0}
        />
        <WorkspaceMetricCard
          detail={t("metric.archivedSessionsDetail")}
          label={t("metric.archivedSessions")}
          value={catalog.summary?.archived_sessions ?? 0}
        />
        <WorkspaceMetricCard
          detail={t("metric.pendingApprovalsDetail")}
          label={t("metric.pendingApprovals")}
          tone={(catalog.summary?.sessions_with_pending_approvals ?? 0) > 0 ? "warning" : "default"}
          value={catalog.summary?.sessions_with_pending_approvals ?? 0}
        />
        <WorkspaceMetricCard
          detail={t("metric.activeRunsDetail")}
          label={t("metric.activeRuns")}
          tone={(catalog.summary?.sessions_with_active_runs ?? 0) > 0 ? "accent" : "default"}
          value={catalog.summary?.sessions_with_active_runs ?? 0}
        />
        <WorkspaceMetricCard
          detail={t("metric.contextFilesDetail")}
          label={t("metric.contextFiles")}
          tone={(catalog.summary?.sessions_with_context_files ?? 0) > 0 ? "accent" : "default"}
          value={catalog.summary?.sessions_with_context_files ?? 0}
        />
      </section>

      <WorkspaceSectionCard description={t("filters.description")} title={t("filters.title")}>
        <div className="workspace-form-grid">
          <TextInputField
            label={t("filters.search")}
            placeholder={t("filters.searchPlaceholder")}
            value={catalog.query}
            onChange={catalog.setQuery}
          />
          <SelectField
            label={t("filters.sort")}
            options={[
              { key: "updated_desc", label: t("filters.sort.updatedDesc") },
              { key: "updated_asc", label: t("filters.sort.updatedAsc") },
              { key: "created_desc", label: t("filters.sort.createdDesc") },
              { key: "created_asc", label: t("filters.sort.createdAsc") },
              { key: "title_asc", label: t("filters.sort.titleAsc") },
            ]}
            value={catalog.sort}
            onChange={(value) =>
              catalog.setSort(
                value as
                  | "updated_desc"
                  | "updated_asc"
                  | "created_desc"
                  | "created_asc"
                  | "title_asc",
              )
            }
          />
          <SelectField
            label={t("filters.titleMode")}
            options={[
              { key: "all", label: t("filters.titleMode.all") },
              { key: "ready", label: t("filters.titleMode.ready") },
              { key: "pending", label: t("filters.titleMode.pending") },
              { key: "failed", label: t("filters.titleMode.failed") },
              { key: "idle", label: t("filters.titleMode.idle") },
            ]}
            value={catalog.titleState}
            onChange={catalog.setTitleState}
          />
          <SelectField
            label={t("filters.titleSource")}
            options={[
              { key: "all", label: t("filters.titleSource.all") },
              { key: "label", label: t("filters.titleSource.label") },
              { key: "semantic_title", label: t("filters.titleSource.semantic") },
              { key: "auto_title", label: t("filters.titleSource.auto") },
              { key: "session_key", label: t("filters.titleSource.sessionKey") },
            ]}
            value={catalog.titleSource}
            onChange={catalog.setTitleSource}
          />
          <SelectField
            label={t("filters.branchState")}
            options={[
              { key: "all", label: t("filters.branchState.all") },
              { key: "root", label: t("filters.branchState.root") },
              { key: "active_branch", label: t("filters.branchState.active") },
              { key: "branch_source", label: t("filters.branchState.source") },
            ]}
            value={catalog.branchState}
            onChange={catalog.setBranchState}
          />
          <SelectField
            label={t("filters.pendingApprovals")}
            options={[
              { key: "all", label: t("filters.pendingApprovals.all") },
              { key: "yes", label: t("filters.pendingApprovals.yes") },
              { key: "no", label: t("filters.pendingApprovals.no") },
            ]}
            value={catalog.hasPendingApprovals}
            onChange={(value) => catalog.setHasPendingApprovals(value as "all" | "yes" | "no")}
          />
          <SelectField
            label={t("filters.contextFiles")}
            options={[
              { key: "all", label: t("filters.contextFiles.all") },
              { key: "yes", label: t("filters.contextFiles.yes") },
              { key: "no", label: t("filters.contextFiles.no") },
            ]}
            value={catalog.hasContextFiles}
            onChange={(value) => catalog.setHasContextFiles(value as "all" | "yes" | "no")}
          />
          <TextInputField
            label={t("filters.agent")}
            placeholder={t("filters.agentPlaceholder")}
            value={catalog.agentId}
            onChange={catalog.setAgentId}
          />
          <TextInputField
            label={t("filters.modelProfile")}
            placeholder={t("filters.modelProfilePlaceholder")}
            value={catalog.modelProfile}
            onChange={catalog.setModelProfile}
          />
          <SwitchField
            checked={catalog.includeArchived}
            description={t("filters.showArchivedDescription")}
            label={t("filters.showArchived")}
            onChange={catalog.setIncludeArchived}
          />
        </div>
      </WorkspaceSectionCard>

      <section className="workspace-two-column">
        <WorkspaceSectionCard description={t("catalog.description")} title={t("catalog.title")}>
          {catalog.entries.length === 0 ? (
            <WorkspaceEmptyState
              description={t("catalog.emptyDescription")}
              title={t("catalog.emptyTitle")}
            />
          ) : (
            <WorkspaceTable
              ariaLabel="Session catalog"
              columns={[
                t("catalog.columns.title"),
                t("catalog.columns.family"),
                t("catalog.columns.updated"),
                t("catalog.columns.controls"),
                t("catalog.columns.recap"),
              ]}
            >
              {catalog.entries.map((entry) => {
                const selectedRow = entry.session_id === catalog.selectedSessionId;
                const entryPresentation = buildSessionCatalogPresentation(entry);
                return (
                  <tr
                    key={entry.session_id}
                    className={selectedRow ? "bg-content2/60" : undefined}
                    onClick={() => catalog.setSelectedSessionId(entry.session_id)}
                  >
                    <td>
                      <div className="workspace-stack">
                        <strong>{entry.title}</strong>
                        <small className="text-muted">
                          {describeTitleGenerationState(
                            entry.title_generation_state,
                            entry.manual_title_locked,
                          )}{" "}
                          · {entry.archived ? t("catalog.archived") : entry.title_source}
                        </small>
                      </div>
                    </td>
                    <td>
                      <div className="workspace-stack">
                        <strong>{entryPresentation.familyRootTitle}</strong>
                        <small className="text-muted">
                          {describeBranchState(entry.branch_state)}
                          {entryPresentation.familySize > 1
                            ? ` · ${entryPresentation.familySequence}/${entryPresentation.familySize}`
                            : ""}
                        </small>
                      </div>
                    </td>
                    <td>{formatUnixMs(entry.updated_at_unix_ms)}</td>
                    <td>
                      <div className="workspace-stack">
                        <small>
                          {entryPresentation.agentDisplay} · {entryPresentation.modelDisplay}
                        </small>
                        <small className="text-muted">
                          {entry.pending_approvals} {t("status.pendingApprovals")}
                          {entryPresentation.activeContextFiles.length > 0
                            ? ` · ${entryPresentation.activeContextFiles.length} context file${entryPresentation.activeContextFiles.length === 1 ? "" : "s"}`
                            : ""}
                        </small>
                      </div>
                    </td>
                    <td>{entry.preview ?? entry.last_summary ?? t("catalog.noRecap")}</td>
                  </tr>
                );
              })}
            </WorkspaceTable>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard description={t("detail.description")} title={t("detail.title")}>
          {selected === null ? (
            <WorkspaceEmptyState
              compact
              description={t("detail.emptyDescription")}
              title={t("detail.emptyTitle")}
            />
          ) : (
            <div className="workspace-stack">
              <div className="workspace-panel__intro">
                <p className="workspace-kicker">{t("detail.selectedSession")}</p>
                <h3>{selected.title}</h3>
                <p className="chat-muted">
                  {selected.preview ?? selected.last_summary ?? t("detail.noPreview")}
                </p>
                <div className="workspace-chip-row">
                  <WorkspaceStatusChip tone={selected.manual_title_locked ? "accent" : "default"}>
                    {describeTitleGenerationState(
                      selected.title_generation_state,
                      selected.manual_title_locked,
                    )}
                  </WorkspaceStatusChip>
                  <WorkspaceStatusChip tone="default">
                    {selectedPresentation.agentDisplay}
                  </WorkspaceStatusChip>
                  <WorkspaceStatusChip tone="default">
                    {selectedPresentation.modelDisplay}
                  </WorkspaceStatusChip>
                  {selectedPresentation.familySize > 1 ? (
                    <WorkspaceStatusChip tone="accent">
                      {t("detail.family", {
                        index: selectedPresentation.familySequence,
                        count: selectedPresentation.familySize,
                      })}
                    </WorkspaceStatusChip>
                  ) : null}
                </div>
              </div>

              <TextInputField
                disabled={catalog.busy}
                description={t("detail.sessionLabelDescription")}
                label={t("detail.sessionLabel")}
                value={catalog.renameDraft}
                onChange={catalog.setRenameDraft}
              />

              <div className="workspace-inline">
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="primary"
                  onPress={() => void catalog.renameSelectedSession()}
                >
                  {t("action.rename")}
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="secondary"
                  onPress={() => void catalog.resetSelectedSession()}
                >
                  {t("action.reset")}
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="danger"
                  onPress={() => void catalog.archiveSelectedSession()}
                >
                  {t("action.archive")}
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || !selected.last_run_id}
                  type="button"
                  variant="ghost"
                  onPress={() => void catalog.abortSelectedRun()}
                >
                  {t("action.abortRun")}
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || phase4Busy !== null}
                  type="button"
                  variant="secondary"
                  onPress={() => void createCheckpoint()}
                >
                  {phase4Busy === "checkpoint"
                    ? t("action.checkpointing")
                    : t("action.createCheckpoint")}
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || phase4Busy !== null}
                  type="button"
                  variant="secondary"
                  onPress={() => void previewCompaction()}
                >
                  {phase4Busy === "preview"
                    ? t("action.previewing")
                    : t("action.previewCompaction")}
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || phase4Busy !== null}
                  type="button"
                  variant="primary"
                  onPress={() => void applyCompaction()}
                >
                  {phase4Busy === "apply" ? t("action.applying") : t("action.applyCompaction")}
                </ActionButton>
              </div>

              <ActionButton
                type="button"
                variant="secondary"
                onPress={() => {
                  if (selectedObjective !== null) {
                    void navigate(
                      buildObjectiveChatHref({
                        objective: selectedObjective,
                        fallbackSessionId: selected.session_id,
                        runId: selected.last_run_id,
                      }),
                    );
                    return;
                  }
                  const search = new URLSearchParams();
                  search.set("sessionId", selected.session_id);
                  if (selected.last_run_id) {
                    search.set("runId", selected.last_run_id);
                  }
                  void navigate(`${getSectionPath("chat")}?${search.toString()}`);
                }}
              >
                {t("action.openChat")}
              </ActionButton>
              <ActionButton
                type="button"
                variant="secondary"
                isDisabled={selectedObjective === null}
                onPress={() => {
                  if (selectedObjective === null) {
                    return;
                  }
                  const objectiveId = readString(selectedObjective, "objective_id");
                  if (objectiveId === null) {
                    return;
                  }
                  void navigate(buildObjectiveOverviewHref(objectiveId));
                }}
              >
                {t("action.openObjective")}
              </ActionButton>
              <ActionButton
                type="button"
                variant="ghost"
                onPress={() =>
                  void navigate(`${getSectionPath("inventory")}?deviceId=${selected.device_id}`)
                }
              >
                {t("action.openInventory")}
              </ActionButton>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>{t("detail.sessionKey")}</dt>
                  <dd>{selected.session_key}</dd>
                </div>
                <div>
                  <dt>{t("detail.titleSource")}</dt>
                  <dd>{selected.title_source}</dd>
                </div>
                <div>
                  <dt>{t("detail.familyRoot")}</dt>
                  <dd>{selectedPresentation.familyRootTitle}</dd>
                </div>
                <div>
                  <dt>{t("detail.created")}</dt>
                  <dd>{formatUnixMs(selected.created_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>{t("detail.updated")}</dt>
                  <dd>{formatUnixMs(selected.updated_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>{t("detail.runState")}</dt>
                  <dd>{selected.last_run_state ?? t("detail.none")}</dd>
                </div>
                <div>
                  <dt>{t("filters.branchState")}</dt>
                  <dd>{describeBranchState(selected.branch_state)}</dd>
                </div>
                <div>
                  <dt>{t("detail.lineage")}</dt>
                  <dd>{selectedLineage}</dd>
                </div>
                <div>
                  <dt>{t("detail.totalTokens")}</dt>
                  <dd>{selected.total_tokens}</dd>
                </div>
                <div>
                  <dt>{t("metric.pendingApprovals")}</dt>
                  <dd>{selected.pending_approvals}</dd>
                </div>
                <div>
                  <dt>{t("detail.contextFiles")}</dt>
                  <dd>
                    {selectedPresentation.activeContextFiles.length > 0
                      ? t("detail.active", {
                          count: selectedPresentation.activeContextFiles.length,
                        })
                      : t("detail.none")}
                  </dd>
                </div>
              </dl>

              {selected.last_intent || selected.last_summary ? (
                <WorkspaceInlineNotice title={t("detail.latestActivity")} tone="default">
                  <p>
                    <strong>{t("detail.lastIntent")}</strong>{" "}
                    {selected.last_intent ?? t("detail.missing")}
                  </p>
                  <p>
                    <strong>{t("detail.lastSummary")}</strong>{" "}
                    {selected.last_summary ?? t("detail.missing")}
                  </p>
                </WorkspaceInlineNotice>
              ) : null}

              {selectedPresentation.touchedFiles.length > 0 ||
              selectedPresentation.activeContextFiles.length > 0 ||
              selectedPresentation.recentArtifacts.length > 0 ? (
                <WorkspaceInlineNotice title={t("detail.resumeRecap")} tone="accent">
                  {selectedPresentation.touchedFiles.length > 0 ? (
                    <p>
                      <strong>{t("detail.touchedFiles")}</strong>{" "}
                      {selectedPresentation.touchedFiles.join(", ")}
                    </p>
                  ) : null}
                  {selectedPresentation.activeContextFiles.length > 0 ? (
                    <p>
                      <strong>{t("detail.activeContext")}</strong>{" "}
                      {selectedPresentation.activeContextFiles.join(", ")}
                    </p>
                  ) : null}
                  {selectedPresentation.recentArtifacts.length > 0 ? (
                    <p>
                      <strong>{t("detail.recentArtifacts")}</strong>{" "}
                      {selectedPresentation.recentArtifacts
                        .map((artifact) => `${artifact.label} (${artifact.kind})`)
                        .join(", ")}
                    </p>
                  ) : null}
                </WorkspaceInlineNotice>
              ) : null}

              <WorkspaceInlineNotice
                title={
                  selectedObjective === null
                    ? t("detail.objectiveLinkage")
                    : t("detail.linkedObjective")
                }
                tone={selectedObjective === null ? "default" : "accent"}
              >
                {selectedObjective === null ? (
                  objectivesBusy ? (
                    <p>{t("detail.loadingObjectiveLinkage")}</p>
                  ) : (
                    <p>{t("detail.noObjective")}</p>
                  )
                ) : (
                  <>
                    <p>
                      <strong>
                        {readString(selectedObjective, "name") ?? t("detail.unnamedObjective")}
                      </strong>{" "}
                      · {readString(selectedObjective, "kind") ?? "objective"} ·{" "}
                      {readString(selectedObjective, "state") ?? "unknown"}
                    </p>
                    <p>
                      <strong>{t("detail.currentFocus")}</strong>{" "}
                      {readString(selectedObjective, "current_focus") ?? t("detail.noCurrentFocus")}
                    </p>
                    <p>
                      <strong>{t("detail.nextAction")}</strong>{" "}
                      {readString(selectedObjective, "next_recommended_step") ??
                        t("detail.noNextAction")}
                    </p>
                  </>
                )}
              </WorkspaceInlineNotice>

              {compactionPreview !== null ? (
                <WorkspaceInlineNotice
                  title={
                    compactionPreview.eligible ? t("continuity.preview") : t("continuity.blocked")
                  }
                  tone={
                    !compactionPreview.eligible
                      ? "warning"
                      : continuityReviewCount > 0
                        ? "warning"
                        : "success"
                  }
                >
                  <p>
                    <strong>{t("continuity.summary")}</strong> {compactionPreview.summary_preview}
                  </p>
                  <p>
                    <strong>{t("continuity.tokenDelta")}</strong> {compactionPreview.token_delta} ·{" "}
                    <strong>{t("continuity.plannedWrites")}</strong> {continuityWriteCount} ·{" "}
                    <strong>{t("continuity.reviewCandidates")}</strong> {continuityReviewCount}
                  </p>
                  {continuityReviewCount > 0 ? <p>{t("continuity.reviewHelp")}</p> : null}
                </WorkspaceInlineNotice>
              ) : null}

              <div className="workspace-inline-actions">
                <WorkspaceStatusChip tone={continuityBusy ? "warning" : "default"}>
                  {continuityBusy
                    ? t("continuity.loading")
                    : t("continuity.compactions", { count: sessionCompactions.length })}
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone={sessionCheckpoints.length > 0 ? "accent" : "default"}>
                  {t("continuity.checkpoints", { count: sessionCheckpoints.length })}
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone={continuityReviewCount > 0 ? "warning" : "default"}>
                  {t("continuity.pendingReview", { count: continuityReviewCount })}
                </WorkspaceStatusChip>
              </div>

              <div className="workspace-stack">
                <div className="workspace-panel__intro">
                  <p className="workspace-kicker">{t("continuity.artifacts")}</p>
                  <h3>{t("continuity.recentCompactions")}</h3>
                  <p className="chat-muted">{t("continuity.recentCompactionsDescription")}</p>
                </div>
                {recentCompactions.length === 0 ? (
                  <WorkspaceEmptyState
                    compact
                    description={t("continuity.emptyCompactionsDescription")}
                    title={t("continuity.emptyCompactionsTitle")}
                  />
                ) : (
                  <div className="chat-ops-list">
                    {recentCompactions.map((artifact) => {
                      const summary = safeParseCompactionSummaryJson(artifact.summary_json);
                      const lifecycleState = summary?.lifecycle_state ?? "stored";
                      const reviewCount = summary?.planner?.review_candidate_count ?? 0;
                      const writeCount = summary?.writes?.length ?? 0;
                      return (
                        <article key={artifact.artifact_id} className="chat-ops-card">
                          <div className="chat-ops-card__copy">
                            <strong>{artifact.mode}</strong>
                            <span>
                              {lifecycleState.replaceAll("_", " ")} ·{" "}
                              {continuityCountLabel("write", writeCount)} · {reviewCount}{" "}
                              {t("continuity.review")}
                            </span>
                            <p>{artifact.summary_preview}</p>
                          </div>
                          <div className="chat-ops-card__actions">
                            <WorkspaceStatusChip tone={reviewCount > 0 ? "warning" : "accent"}>
                              {artifact.strategy}
                            </WorkspaceStatusChip>
                            <ActionButton
                              size="sm"
                              type="button"
                              variant="ghost"
                              onPress={() =>
                                openChatWithArtifact({
                                  runId: artifact.run_id,
                                  compactionId: artifact.artifact_id,
                                })
                              }
                            >
                              {t("action.openChat")}
                            </ActionButton>
                          </div>
                        </article>
                      );
                    })}
                  </div>
                )}
              </div>

              <div className="workspace-stack">
                <div className="workspace-panel__intro">
                  <p className="workspace-kicker">{t("continuity.recoveryPoints")}</p>
                  <h3>{t("continuity.recentCheckpoints")}</h3>
                  <p className="chat-muted">{t("continuity.recentCheckpointsDescription")}</p>
                </div>
                {recentCheckpoints.length === 0 ? (
                  <WorkspaceEmptyState
                    compact
                    description={t("continuity.emptyCheckpointsDescription")}
                    title={t("continuity.emptyCheckpointsTitle")}
                  />
                ) : (
                  <div className="chat-ops-list">
                    {recentCheckpoints.map((checkpoint) => (
                      <article key={checkpoint.checkpoint_id} className="chat-ops-card">
                        <div className="chat-ops-card__copy">
                          <strong>{checkpoint.name}</strong>
                          <span>
                            {describeBranchState(checkpoint.branch_state)} ·{" "}
                            {t("continuity.restores")} {checkpoint.restore_count}
                          </span>
                          <p>{checkpoint.note ?? t("continuity.noCheckpointNote")}</p>
                        </div>
                        <div className="chat-ops-card__actions">
                          <WorkspaceStatusChip tone="accent">
                            {formatUnixMs(checkpoint.created_at_unix_ms)}
                          </WorkspaceStatusChip>
                          <ActionButton
                            size="sm"
                            type="button"
                            variant="ghost"
                            onPress={() =>
                              openChatWithArtifact({
                                runId: checkpoint.run_id,
                                checkpointId: checkpoint.checkpoint_id,
                              })
                            }
                          >
                            {t("action.openChat")}
                          </ActionButton>
                        </div>
                      </article>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

type ContinuitySummary = {
  lifecycle_state?: string;
  planner?: { review_candidate_count?: number };
  writes?: Array<unknown>;
};

function readCompactionSummary(value: unknown): ContinuitySummary | undefined {
  if (value === null || value === undefined || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  return value as ContinuitySummary;
}

function safeParseCompactionSummaryJson(
  value: string | null | undefined,
): ContinuitySummary | undefined {
  if (typeof value !== "string" || value.trim().length === 0) {
    return undefined;
  }
  try {
    return JSON.parse(value) as ContinuitySummary;
  } catch {
    return undefined;
  }
}
