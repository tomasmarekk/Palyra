import { useEffect, useMemo, useState } from "react";

import type {
  ConsoleApiClient,
  InventoryDeviceDetailEnvelope,
  InventoryDeviceRecord,
  InventoryInstanceRecord,
  InventoryListEnvelope,
  JsonValue,
  NodePairingRequestView,
  NodeInvokeEnvelope,
} from "../../consoleApi";
import { toErrorMessage } from "../shared";

type UseInventoryDomainArgs = {
  api: ConsoleApiClient;
  preferredDeviceId?: string;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

export function useInventoryDomain({
  api,
  preferredDeviceId,
  setError,
  setNotice,
}: UseInventoryDomainArgs) {
  const [busy, setBusy] = useState(false);
  const [detailBusy, setDetailBusy] = useState(false);
  const [summary, setSummary] = useState<InventoryListEnvelope["summary"] | null>(null);
  const [devices, setDevices] = useState<InventoryDeviceRecord[]>([]);
  const [instances, setInstances] = useState<InventoryInstanceRecord[]>([]);
  const [pendingPairings, setPendingPairings] = useState<NodePairingRequestView[]>([]);
  const [selectedDeviceId, setSelectedDeviceId] = useState(preferredDeviceId ?? "");
  const [selectedDetail, setSelectedDetail] = useState<InventoryDeviceDetailEnvelope | null>(null);
  const [actionReason, setActionReason] = useState("");
  const [invokeCapability, setInvokeCapability] = useState("");
  const [invokeInputJson, setInvokeInputJson] = useState("{}");
  const [invokeResult, setInvokeResult] = useState<NodeInvokeEnvelope | null>(null);

  const selectedListRecord = useMemo(
    () => devices.find((record) => record.device_id === selectedDeviceId) ?? null,
    [devices, selectedDeviceId],
  );
  const selectedDevice = selectedDetail?.device ?? selectedListRecord;
  const selectedPairings = useMemo(() => {
    if (selectedDetail !== null) {
      return selectedDetail.pairings;
    }
    if (selectedDeviceId.trim().length === 0) {
      return [];
    }
    return pendingPairings.filter((record) => record.device_id === selectedDeviceId);
  }, [pendingPairings, selectedDetail, selectedDeviceId]);

  useEffect(() => {
    void refreshInventory(preferredDeviceId);
  }, [preferredDeviceId]);

  useEffect(() => {
    if (selectedDeviceId.trim().length === 0) {
      setSelectedDetail(null);
      setInvokeResult(null);
      return;
    }
    void loadDeviceDetail(selectedDeviceId);
  }, [selectedDeviceId]);

  useEffect(() => {
    if (selectedDevice === null) {
      setInvokeCapability("");
      return;
    }
    const availableCapabilities = selectedDevice.capabilities.filter((entry) => entry.available);
    setInvokeCapability((previous) => {
      if (previous.length > 0 && availableCapabilities.some((entry) => entry.name === previous)) {
        return previous;
      }
      return availableCapabilities[0]?.name ?? "";
    });
  }, [selectedDevice]);

  async function refreshInventory(preferredSelection?: string): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      const response = await api.listInventory();
      setSummary(response.summary);
      setDevices(response.devices);
      setInstances(response.instances);
      setPendingPairings(response.pending_pairings);
      setSelectedDeviceId((previous) => {
        const requested =
          (preferredSelection?.trim().length ?? 0) > 0 ? preferredSelection!.trim() : previous.trim();
        if (requested.length > 0 && response.devices.some((record) => record.device_id === requested)) {
          return requested;
        }
        return response.devices[0]?.device_id ?? "";
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function loadDeviceDetail(deviceId: string): Promise<void> {
    setDetailBusy(true);
    setError(null);
    try {
      const response = await api.getInventoryDevice(deviceId);
      setSelectedDetail(response);
    } catch (error) {
      setSelectedDetail(null);
      setError(toErrorMessage(error));
    } finally {
      setDetailBusy(false);
    }
  }

  async function rotateSelectedDevice(): Promise<void> {
    if (selectedDevice === null || !selectedDevice.actions.can_rotate) {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await api.rotateDevice(selectedDevice.device_id);
      setNotice(`Rotated certificate for ${selectedDevice.device_id}.`);
      await refreshInventory(selectedDevice.device_id);
      await loadDeviceDetail(selectedDevice.device_id);
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function revokeSelectedDevice(): Promise<void> {
    if (selectedDevice === null || !selectedDevice.actions.can_revoke) {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await api.revokeDevice(selectedDevice.device_id, {
        reason: actionReason.trim().length > 0 ? actionReason.trim() : undefined,
      });
      setNotice(`Revoked device ${selectedDevice.device_id}.`);
      await refreshInventory(selectedDevice.device_id);
      await loadDeviceDetail(selectedDevice.device_id);
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function removeSelectedDevice(): Promise<void> {
    if (selectedDevice === null || !selectedDevice.actions.can_remove) {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await api.removeDevice(selectedDevice.device_id, {
        reason: actionReason.trim().length > 0 ? actionReason.trim() : undefined,
      });
      setNotice(`Removed device ${selectedDevice.device_id}.`);
      await refreshInventory();
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function invokeSelectedNode(): Promise<void> {
    if (
      selectedDevice === null ||
      !selectedDevice.actions.can_invoke ||
      invokeCapability.trim().length === 0
    ) {
      return;
    }

    let inputJson: JsonValue | undefined = undefined;
    if (invokeInputJson.trim().length > 0) {
      try {
        inputJson = JSON.parse(invokeInputJson) as JsonValue;
      } catch {
        setError("Invoke payload must be valid JSON.");
        return;
      }
    }

    setBusy(true);
    setError(null);
    setInvokeResult(null);
    try {
      const response = await api.invokeNode(selectedDevice.device_id, {
        capability: invokeCapability,
        input_json: inputJson,
      });
      setInvokeResult(response);
      setNotice(
        response.success
          ? `Capability ${response.capability} completed for ${selectedDevice.device_id}.`
          : `Capability ${response.capability} reported an error for ${selectedDevice.device_id}.`,
      );
      await refreshInventory(selectedDevice.device_id);
      await loadDeviceDetail(selectedDevice.device_id);
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  return {
    busy,
    detailBusy,
    summary,
    devices,
    instances,
    pendingPairings,
    selectedDeviceId,
    setSelectedDeviceId,
    selectedDevice,
    selectedPairings,
    actionReason,
    setActionReason,
    invokeCapability,
    setInvokeCapability,
    invokeInputJson,
    setInvokeInputJson,
    invokeResult,
    refreshInventory,
    rotateSelectedDevice,
    revokeSelectedDevice,
    removeSelectedDevice,
    invokeSelectedNode,
  };
}
