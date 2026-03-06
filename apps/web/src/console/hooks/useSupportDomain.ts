import { useState } from "react";

import type { ConsoleApiClient, JsonValue } from "../../consoleApi";
import type { JsonObject } from "../shared";
import { emptyToUndefined, isJsonObject, parseInteger, readString, toErrorMessage, toJsonObjectArray } from "../shared";

type UseSupportDomainArgs = {
  api: ConsoleApiClient;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
};

export function useSupportDomain({ api, setError, setNotice }: UseSupportDomainArgs) {
  const [supportBusy, setSupportBusy] = useState(false);
  const [supportPairingSummary, setSupportPairingSummary] = useState<JsonObject | null>(null);
  const [supportPairingChannel, setSupportPairingChannel] = useState("discord:default");
  const [supportPairingIssuedBy, setSupportPairingIssuedBy] = useState("");
  const [supportPairingTtlMs, setSupportPairingTtlMs] = useState("600000");
  const [supportBundleJobs, setSupportBundleJobs] = useState<JsonObject[]>([]);

  async function refreshSupport(): Promise<void> {
    setSupportBusy(true);
    setError(null);
    try {
      const [pairingResponse, jobsResponse] = await Promise.all([
        api.getPairingSummary(),
        api.listSupportBundleJobs()
      ]);
      setSupportPairingSummary(
        isJsonObject(pairingResponse as unknown as JsonValue) ? (pairingResponse as unknown as JsonObject) : null
      );
      setSupportBundleJobs(toJsonObjectArray(jobsResponse.jobs as unknown as JsonValue[]));
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
    setSupportBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.createSupportBundleJob();
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

  function resetSupportDomain(): void {
    setSupportBusy(false);
    setSupportPairingSummary(null);
    setSupportPairingChannel("discord:default");
    setSupportPairingIssuedBy("");
    setSupportPairingTtlMs("600000");
    setSupportBundleJobs([]);
  }

  return {
    supportBusy,
    supportPairingSummary,
    supportPairingChannel,
    setSupportPairingChannel,
    supportPairingIssuedBy,
    setSupportPairingIssuedBy,
    supportPairingTtlMs,
    setSupportPairingTtlMs,
    supportBundleJobs,
    refreshSupport,
    mintSupportPairingCode,
    createSupportBundle,
    resetSupportDomain
  };
}
