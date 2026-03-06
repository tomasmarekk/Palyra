import { useState } from "react";

import type { ConsoleApiClient, JsonValue } from "../../consoleApi";
import { isJsonObject, toErrorMessage, toJsonObjectArray, type JsonObject } from "../shared";

type UseOverviewDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
};

export function useOverviewDomain({ api, setError }: UseOverviewDomainArgs) {
  const [overviewBusy, setOverviewBusy] = useState(false);
  const [overviewCatalog, setOverviewCatalog] = useState<JsonObject | null>(null);
  const [overviewDeployment, setOverviewDeployment] = useState<JsonObject | null>(null);
  const [overviewSupportJobs, setOverviewSupportJobs] = useState<JsonObject[]>([]);

  async function refreshOverview(): Promise<void> {
    setOverviewBusy(true);
    setError(null);
    try {
      const [catalog, deployment, jobs] = await Promise.all([
        api.getCapabilityCatalog(),
        api.getDeploymentPosture(),
        api.listSupportBundleJobs()
      ]);
      setOverviewCatalog(isJsonObject(catalog as unknown as JsonValue) ? (catalog as unknown as JsonObject) : null);
      setOverviewDeployment(
        isJsonObject(deployment as unknown as JsonValue) ? (deployment as unknown as JsonObject) : null
      );
      setOverviewSupportJobs(toJsonObjectArray(jobs.jobs as unknown as JsonValue[]));
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setOverviewBusy(false);
    }
  }

  function resetOverviewDomain(): void {
    setOverviewBusy(false);
    setOverviewCatalog(null);
    setOverviewDeployment(null);
    setOverviewSupportJobs([]);
  }

  return {
    overviewBusy,
    overviewCatalog,
    overviewDeployment,
    overviewSupportJobs,
    refreshOverview,
    resetOverviewDomain
  };
}
