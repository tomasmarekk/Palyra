import { useEffect, useMemo, useState } from "react";

import type {
  ConsoleApiClient,
  UsageAgentRecord,
  UsageInsightsEnvelope,
  UsageModelRecord,
  UsageSessionDetailEnvelope,
  UsageSessionRecord,
  UsageSummaryEnvelope,
} from "../../consoleApi";
import { toErrorMessage } from "../shared";

type UsageWindowKey = "24h" | "7d" | "30d" | "90d";
type UsageBucketKey = "auto" | "hour" | "day";

type UseUsageDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

export function useUsageDomain({ api, setError, setNotice }: UseUsageDomainArgs) {
  const [busy, setBusy] = useState(false);
  const [windowKey, setWindowKey] = useState<UsageWindowKey>("30d");
  const [bucket, setBucket] = useState<UsageBucketKey>("auto");
  const [includeArchived, setIncludeArchived] = useState(false);
  const [summary, setSummary] = useState<UsageSummaryEnvelope | null>(null);
  const [insights, setInsights] = useState<UsageInsightsEnvelope | null>(null);
  const [sessions, setSessions] = useState<UsageSessionRecord[]>([]);
  const [agents, setAgents] = useState<UsageAgentRecord[]>([]);
  const [models, setModels] = useState<UsageModelRecord[]>([]);
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [selectedSessionDetail, setSelectedSessionDetail] =
    useState<UsageSessionDetailEnvelope | null>(null);

  const selectedSession = useMemo(
    () => sessions.find((entry) => entry.session_id === selectedSessionId) ?? null,
    [sessions, selectedSessionId],
  );

  useEffect(() => {
    void refreshUsage({ clearError: false });
  }, [windowKey, bucket, includeArchived]);

  useEffect(() => {
    void refreshSelectedSessionDetail({ clearError: false });
  }, [selectedSessionId, windowKey, bucket, includeArchived]);

  function buildParams(now = Date.now()): URLSearchParams {
    const params = new URLSearchParams();
    params.set("end_at_unix_ms", now.toString());
    params.set("start_at_unix_ms", Math.max(0, now - usageWindowMs(windowKey)).toString());
    if (bucket !== "auto") {
      params.set("bucket", bucket);
    }
    if (includeArchived) {
      params.set("include_archived", "true");
    }
    return params;
  }

  async function refreshUsage(options?: { clearError?: boolean }): Promise<void> {
    const now = Date.now();
    const params = buildParams(now);
    const topParams = new URLSearchParams(params);
    topParams.set("limit", "8");

    setBusy(true);
    if (options?.clearError !== false) {
      setError(null);
    }
    try {
      const [nextSummary, nextInsights, nextSessions, nextAgents, nextModels] = await Promise.all([
        api.getUsageSummary(params),
        api.getUsageInsights(params),
        api.listUsageSessions(topParams),
        api.listUsageAgents(topParams),
        api.listUsageModels(topParams),
      ]);
      setSummary(nextSummary);
      setInsights(nextInsights);
      setSessions(nextSessions.sessions);
      setAgents(nextAgents.agents);
      setModels(nextModels.models);
      setSelectedSessionId((previous) => {
        if (
          previous.length > 0 &&
          nextSessions.sessions.some((entry) => entry.session_id === previous)
        ) {
          return previous;
        }
        return nextSessions.sessions[0]?.session_id ?? "";
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function refreshSelectedSessionDetail(options?: { clearError?: boolean }): Promise<void> {
    if (selectedSessionId.trim().length === 0) {
      setSelectedSessionDetail(null);
      return;
    }
    try {
      if (options?.clearError !== false) {
        setError(null);
      }
      const params = buildParams();
      params.set("run_limit", "12");
      const detail = await api.getUsageSessionDetail(selectedSessionId, params);
      setSelectedSessionDetail(detail);
    } catch (error) {
      setSelectedSessionDetail(null);
      setError(toErrorMessage(error));
    }
  }

  function exportDataset(
    dataset: "timeline" | "sessions" | "agents" | "models",
    format: "csv" | "json",
  ) {
    const params = buildParams();
    params.set("dataset", dataset);
    params.set("format", format);
    window.open(
      api.resolvePath(`/console/v1/usage/export?${params.toString()}`),
      "_blank",
      "noopener",
    );
    setNotice(`Export started for ${dataset} (${format.toUpperCase()}).`);
  }

  async function requestBudgetOverride(policyId: string): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      const response = await api.requestUsageBudgetOverride(policyId);
      setNotice(`Budget override requested for ${response.policy.policy_id}.`);
      await refreshUsage();
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  return {
    busy,
    windowKey,
    setWindowKey,
    bucket,
    setBucket,
    includeArchived,
    setIncludeArchived,
    summary,
    insights,
    sessions,
    agents,
    models,
    selectedSessionId,
    setSelectedSessionId,
    selectedSession,
    selectedSessionDetail,
    refreshUsage,
    exportDataset,
    requestBudgetOverride,
  };
}

function usageWindowMs(windowKey: UsageWindowKey): number {
  switch (windowKey) {
    case "24h":
      return 24 * 60 * 60 * 1000;
    case "7d":
      return 7 * 24 * 60 * 60 * 1000;
    case "30d":
      return 30 * 24 * 60 * 60 * 1000;
    case "90d":
      return 90 * 24 * 60 * 60 * 1000;
  }
}
