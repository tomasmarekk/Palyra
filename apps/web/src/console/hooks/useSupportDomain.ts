import { useState } from "react";

import type { ConsoleApiClient, JsonValue } from "../../consoleApi";
import type { JsonObject } from "../shared";
import {
  emptyToUndefined,
  isJsonObject,
  parseInteger,
  readString,
  toErrorMessage,
  toJsonObjectArray
} from "../shared";

type UseSupportDomainArgs = {
  api: ConsoleApiClient;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
};

export function useSupportDomain({ api, setError, setNotice }: UseSupportDomainArgs) {
  const [supportBusy, setSupportBusy] = useState(false);
  const [supportPairingSummary, setSupportPairingSummary] = useState<JsonObject | null>(null);
  const [supportDeployment, setSupportDeployment] = useState<JsonObject | null>(null);
  const [supportDiagnosticsSnapshot, setSupportDiagnosticsSnapshot] = useState<JsonObject | null>(null);
  const [supportPairingChannel, setSupportPairingChannel] = useState("discord:default");
  const [supportPairingIssuedBy, setSupportPairingIssuedBy] = useState("");
  const [supportPairingTtlMs, setSupportPairingTtlMs] = useState("600000");
  const [supportBundleRetainJobs, setSupportBundleRetainJobs] = useState("16");
  const [supportBundleJobs, setSupportBundleJobs] = useState<JsonObject[]>([]);
  const [supportSelectedBundleJobId, setSupportSelectedBundleJobId] = useState("");
  const [supportSelectedBundleJob, setSupportSelectedBundleJob] = useState<JsonObject | null>(null);

  async function refreshSupport(): Promise<void> {
    setSupportBusy(true);
    setError(null);
    try {
      const [pairingResponse, jobsResponse, deploymentResponse, diagnosticsResponse] = await Promise.all([
        api.getPairingSummary(),
        api.listSupportBundleJobs(),
        api.getDeploymentPosture(),
        api.getDiagnostics()
      ]);
      setSupportPairingSummary(
        isJsonObject(pairingResponse as unknown as JsonValue) ? (pairingResponse as unknown as JsonObject) : null
      );
      setSupportBundleJobs(toJsonObjectArray(jobsResponse.jobs as unknown as JsonValue[]));
      setSupportDeployment(
        isJsonObject(deploymentResponse as unknown as JsonValue)
          ? (deploymentResponse as unknown as JsonObject)
          : null
      );
      setSupportDiagnosticsSnapshot(
        isJsonObject(diagnosticsResponse as unknown as JsonValue)
          ? (diagnosticsResponse as unknown as JsonObject)
          : null
      );
      const nextSelectedJobId = supportSelectedBundleJobId.trim();
      if (nextSelectedJobId.length > 0) {
        const jobResponse = await api.getSupportBundleJob(nextSelectedJobId);
        setSupportSelectedBundleJob(
          isJsonObject(jobResponse.job as unknown as JsonValue)
            ? (jobResponse.job as unknown as JsonObject)
            : null
        );
      }
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSupportBusy(false);
    }
  }

  async function mintSupportPairingCode(): Promise<void> {
    if (supportPairingChannel.trim().length === 0) {
      setError("Pairing channel cannot be empty.");
      return;
    }
    const ttlMs = parseInteger(supportPairingTtlMs);
    if (ttlMs === null || ttlMs <= 0) {
      setError("Pairing TTL must be a positive integer.");
      return;
    }
    setSupportBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.mintPairingCode({
        channel: supportPairingChannel.trim(),
        issued_by: emptyToUndefined(supportPairingIssuedBy),
        ttl_ms: ttlMs
      });
      setSupportPairingSummary(
        isJsonObject(response as unknown as JsonValue) ? (response as unknown as JsonObject) : null
      );
      setNotice("Pairing code minted.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSupportBusy(false);
    }
  }

  async function createSupportBundle(): Promise<void> {
    const retainJobs = parseInteger(supportBundleRetainJobs);
    if (retainJobs === null || retainJobs <= 0) {
      setError("Retain jobs must be a positive integer.");
      return;
    }
    setSupportBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.createSupportBundleJob({ retain_jobs: retainJobs });
      const createdJob =
        isJsonObject(response.job as unknown as JsonValue) ? (response.job as unknown as JsonObject) : null;
      setSupportSelectedBundleJob(createdJob);
      setSupportSelectedBundleJobId(readString(createdJob ?? {}, "job_id") ?? "");
      setNotice(
        `Support bundle job queued: ${readString(response.job as unknown as JsonObject, "job_id") ?? "unknown"}.`
      );
      await refreshSupport();
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSupportBusy(false);
    }
  }

  async function loadSupportBundleJob(): Promise<void> {
    const jobId = supportSelectedBundleJobId.trim();
    if (jobId.length === 0) {
      setError("Support bundle job ID cannot be empty.");
      return;
    }
    setSupportBusy(true);
    setError(null);
    try {
      const response = await api.getSupportBundleJob(jobId);
      setSupportSelectedBundleJob(
        isJsonObject(response.job as unknown as JsonValue) ? (response.job as unknown as JsonObject) : null
      );
      setNotice("Support bundle job refreshed.");
    } catch (failure) {
      setError(toErrorMessage(failure));
    } finally {
      setSupportBusy(false);
    }
  }

  function resetSupportDomain(): void {
    setSupportBusy(false);
    setSupportPairingSummary(null);
    setSupportDeployment(null);
    setSupportDiagnosticsSnapshot(null);
    setSupportPairingChannel("discord:default");
    setSupportPairingIssuedBy("");
    setSupportPairingTtlMs("600000");
    setSupportBundleRetainJobs("16");
    setSupportBundleJobs([]);
    setSupportSelectedBundleJobId("");
    setSupportSelectedBundleJob(null);
  }

  return {
    supportBusy,
    supportPairingSummary,
    supportDeployment,
    supportDiagnosticsSnapshot,
    supportPairingChannel,
    setSupportPairingChannel,
    supportPairingIssuedBy,
    setSupportPairingIssuedBy,
    supportPairingTtlMs,
    setSupportPairingTtlMs,
    supportBundleRetainJobs,
    setSupportBundleRetainJobs,
    supportBundleJobs,
    supportSelectedBundleJobId,
    setSupportSelectedBundleJobId,
    supportSelectedBundleJob,
    refreshSupport,
    mintSupportPairingCode,
    createSupportBundle,
    loadSupportBundleJob,
    resetSupportDomain
  };
}
