import { useState } from "react";

import type { ConsoleApiClient, JsonValue, UsageInsightsEnvelope } from "../../consoleApi";
import { isJsonObject, toErrorMessage, toJsonObjectArray, type JsonObject } from "../shared";

type UseOverviewDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
};

export function useOverviewDomain({ api, setError }: UseOverviewDomainArgs) {
  const [overviewBusy, setOverviewBusy] = useState(false);
  const [overviewCatalog, setOverviewCatalog] = useState<JsonObject | null>(null);
  const [overviewDeployment, setOverviewDeployment] = useState<JsonObject | null>(null);
  const [overviewApprovals, setOverviewApprovals] = useState<JsonObject[]>([]);
  const [overviewDiagnostics, setOverviewDiagnostics] = useState<JsonObject | null>(null);
  const [overviewUsageInsights, setOverviewUsageInsights] = useState<UsageInsightsEnvelope | null>(
    null,
  );
  const [overviewSupportJobs, setOverviewSupportJobs] = useState<JsonObject[]>([]);

  async function refreshOverview(): Promise<void> {
    setOverviewBusy(true);
    setError(null);
    const [catalog, deployment, approvals, diagnostics, usageInsights, jobs] =
      await Promise.allSettled([
        api.getCapabilityCatalog(),
        api.getDeploymentPosture(),
        api.listApprovals(),
        api.getDiagnostics(),
        api.getUsageInsights(),
        api.listSupportBundleJobs(),
      ]);

    if (catalog.status === "fulfilled") {
      setOverviewCatalog(
        isJsonObject(catalog.value as unknown as JsonValue)
          ? (catalog.value as unknown as JsonObject)
          : null,
      );
    }
    if (deployment.status === "fulfilled") {
      setOverviewDeployment(
        isJsonObject(deployment.value as unknown as JsonValue)
          ? (deployment.value as unknown as JsonObject)
          : null,
      );
    }
    if (approvals.status === "fulfilled") {
      setOverviewApprovals(
        toJsonObjectArray(
          Array.isArray(approvals.value.approvals) ? approvals.value.approvals : [],
        ),
      );
    }
    if (diagnostics.status === "fulfilled") {
      setOverviewDiagnostics(
        isJsonObject(diagnostics.value as unknown as JsonValue)
          ? (diagnostics.value as unknown as JsonObject)
          : null,
      );
    }
    if (usageInsights.status === "fulfilled") {
      setOverviewUsageInsights(normalizeUsageInsightsEnvelope(usageInsights.value));
    }
    if (jobs.status === "fulfilled") {
      setOverviewSupportJobs(
        toJsonObjectArray(
          Array.isArray(jobs.value.jobs) ? (jobs.value.jobs as unknown as JsonValue[]) : [],
        ),
      );
    }

    const firstFailure = firstRejectedReason([catalog, deployment, jobs]);
    if (firstFailure !== undefined) {
      setError(toErrorMessage(firstFailure));
    }
    setOverviewBusy(false);
  }

  function resetOverviewDomain(): void {
    setOverviewBusy(false);
    setOverviewCatalog(null);
    setOverviewDeployment(null);
    setOverviewApprovals([]);
    setOverviewDiagnostics(null);
    setOverviewUsageInsights(null);
    setOverviewSupportJobs([]);
  }

  return {
    overviewBusy,
    overviewCatalog,
    overviewDeployment,
    overviewApprovals,
    overviewDiagnostics,
    overviewUsageInsights,
    overviewSupportJobs,
    refreshOverview,
    resetOverviewDomain,
  };
}

function normalizeUsageInsightsEnvelope(value: UsageInsightsEnvelope): UsageInsightsEnvelope {
  return {
    ...value,
    timeline: Array.isArray(value.timeline) ? value.timeline : [],
    routing: {
      ...value.routing,
      recent_decisions: Array.isArray(value.routing?.recent_decisions)
        ? value.routing.recent_decisions
        : [],
    },
    budgets: {
      ...value.budgets,
      policies: Array.isArray(value.budgets?.policies) ? value.budgets.policies : [],
      evaluations: Array.isArray(value.budgets?.evaluations) ? value.budgets.evaluations : [],
    },
    alerts: Array.isArray(value.alerts) ? value.alerts : [],
    model_mix: Array.isArray(value.model_mix) ? value.model_mix : [],
    scope_mix: Array.isArray(value.scope_mix) ? value.scope_mix : [],
    tool_mix: Array.isArray(value.tool_mix) ? value.tool_mix : [],
  };
}

function firstRejectedReason(results: ReadonlyArray<PromiseSettledResult<unknown>>): unknown {
  for (const result of results) {
    if (result.status === "rejected") {
      return result.reason;
    }
  }
  return undefined;
}
