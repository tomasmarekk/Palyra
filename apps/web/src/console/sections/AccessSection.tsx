import { useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import type {
  CapabilityCatalog,
  DeploymentPostureSummary,
  InventoryDeviceRecord,
  NodePairingCodeView,
  NodePairingMethod,
  NodePairingRequestView,
} from "../../consoleApi";
import { getSectionPath } from "../navigation";
import { useInventoryDomain } from "../hooks/useInventoryDomain";
import { ActionButton, SelectField, TextInputField } from "../components/ui";
import { CapabilityCardList } from "../components/CapabilityCards";
import { AccessControlWorkspace } from "./access/AccessControlWorkspace";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceConfirmDialog,
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import { formatUnixMs, type JsonObject } from "../shared";
import { capabilitiesByMode, capabilitiesForSection } from "../capabilityCatalog";
import type { ConsoleAppState } from "../useConsoleAppState";

type AccessSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "api"
    | "setError"
    | "setNotice"
    | "supportBusy"
    | "supportDeployment"
    | "supportNodePairingMethod"
    | "setSupportNodePairingMethod"
    | "supportPairingIssuedBy"
    | "setSupportPairingIssuedBy"
    | "supportPairingTtlMs"
    | "setSupportPairingTtlMs"
    | "supportNodePairingCodes"
    | "supportNodePairingRequests"
    | "supportPairingDecisionReason"
    | "setSupportPairingDecisionReason"
    | "refreshSupport"
    | "mintSupportPairingCode"
    | "approveSupportPairingRequest"
    | "rejectSupportPairingRequest"
    | "overviewCatalog"
    | "setSection"
  >;
};

type PendingDecision = { action: "approve" | "reject"; request: NodePairingRequestView };
type TrustAction = "rotate" | "revoke" | "remove";

export function AccessSection({ app }: AccessSectionProps) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const catalog = readCapabilityCatalog(app.overviewCatalog);
  const groupedCapabilities = capabilitiesByMode(capabilitiesForSection(catalog, "access"));
  const deployment = app.supportDeployment as unknown as DeploymentPostureSummary | null;
  const preferredDeviceId =
    searchParams.get("deviceId") ??
    app.supportNodePairingRequests.find((record) => record.state === "pending_approval")?.device_id;
  const inventory = useInventoryDomain({
    api: app.api,
    preferredDeviceId,
    setError: app.setError,
    setNotice: app.setNotice,
  });
  const [selectedRequestId, setSelectedRequestId] = useState(
    searchParams.get("requestId") ??
      app.supportNodePairingRequests.find((record) => record.state === "pending_approval")
        ?.request_id ??
      "",
  );
  const [pendingDecision, setPendingDecision] = useState<PendingDecision | null>(null);
  const [pendingTrustAction, setPendingTrustAction] = useState<TrustAction | null>(null);

  const pendingRequests = app.supportNodePairingRequests.filter(
    (record) => record.state === "pending_approval",
  );
  const selectedRequest =
    app.supportNodePairingRequests.find((record) => record.request_id === selectedRequestId) ??
    pendingRequests[0] ??
    app.supportNodePairingRequests[0] ??
    null;
  const selectedTrustDevice = inventory.selectedDevice;
  const trustAttentionCount = inventory.devices.filter(
    (record) => record.presence_state !== "ok" || record.trust_state !== "trusted",
  ).length;
  const stalePendingCount = pendingRequests.filter((record) => {
    const related = inventory.devices.find((device) => device.device_id === record.device_id);
    return related !== undefined && related.presence_state !== "ok";
  }).length;
  const accessBusy = app.supportBusy || inventory.busy || inventory.detailBusy;
  const postureWarnings = Array.isArray(deployment?.warnings) ? deployment.warnings : [];
  const remoteChecklist = useMemo(
    () => buildRemoteChecklist(deployment, inventory.devices),
    [deployment, inventory.devices],
  );

  async function refreshAccess(): Promise<void> {
    await Promise.all([
      app.refreshSupport(),
      inventory.refreshInventory(inventory.selectedDeviceId),
    ]);
  }

  async function confirmDecision(): Promise<void> {
    if (pendingDecision === null) return;
    const { action, request } = pendingDecision;
    setPendingDecision(null);
    if (action === "approve") {
      await app.approveSupportPairingRequest(request.request_id);
      return;
    }
    await app.rejectSupportPairingRequest(request.request_id);
  }

  async function confirmTrustAction(): Promise<void> {
    const action = pendingTrustAction;
    setPendingTrustAction(null);
    if (action === "rotate") return inventory.rotateSelectedDevice();
    if (action === "revoke") return inventory.revokeSelectedDevice();
    if (action === "remove") return inventory.removeSelectedDevice();
  }

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Settings"
        title="Access"
        description="Guide node pairing, remote verify, and trust review in one place so routine operator paths stay web-first while risky seams remain explicit."
        status={
          <>
            <WorkspaceStatusChip tone={accessBusy ? "warning" : "success"}>
              {accessBusy ? "Refreshing access" : "Access ready"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={pendingRequests.length > 0 ? "warning" : "default"}>
              {pendingRequests.length} pending approvals
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={trustAttentionCount > 0 ? "warning" : "default"}>
              {trustAttentionCount} trust attention states
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            isDisabled={accessBusy}
            type="button"
            variant="primary"
            onPress={() => void refreshAccess()}
          >
            {accessBusy ? "Refreshing..." : "Refresh access"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Devices"
          value={inventory.summary?.devices ?? inventory.devices.length}
          detail="Nodes and devices currently visible through the inventory trust surface."
        />
        <WorkspaceMetricCard
          label="Pending requests"
          value={pendingRequests.length}
          tone={pendingRequests.length > 0 ? "warning" : "default"}
          detail="Node pairing requests still waiting on operator approval."
        />
        <WorkspaceMetricCard
          label="Active codes"
          value={app.supportNodePairingCodes.length}
          tone={app.supportNodePairingCodes.length > 0 ? "success" : "default"}
          detail="Pairing codes that can still bootstrap a node or companion."
        />
        <WorkspaceMetricCard
          label="Trust attention"
          value={trustAttentionCount}
          tone={trustAttentionCount > 0 ? "warning" : "default"}
          detail="Devices that are stale, offline, revoked, or otherwise outside the trusted happy path."
        />
      </section>

      <AccessControlWorkspace api={app.api} setError={app.setError} setNotice={app.setNotice} />

      {(postureWarnings.length > 0 || stalePendingCount > 0) && (
        <WorkspaceInlineNotice title="Guidance" tone="warning">
          <ul className="console-compact-list">
            {postureWarnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
            {stalePendingCount > 0 && (
              <li>
                {stalePendingCount} pending request{stalePendingCount === 1 ? "" : "s"} map to
                devices that are already stale, degraded, or offline.
              </li>
            )}
          </ul>
        </WorkspaceInlineNotice>
      )}

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <PairingWizardCard app={app} accessBusy={accessBusy} />
          <PendingApprovalsCard
            accessBusy={accessBusy}
            app={app}
            devices={inventory.devices}
            requests={app.supportNodePairingRequests}
            selectedRequest={selectedRequest}
            onApprove={(request) => {
              setSelectedRequestId(request.request_id);
              setPendingDecision({ action: "approve", request });
            }}
            onReject={(request) => {
              setSelectedRequestId(request.request_id);
              setPendingDecision({ action: "reject", request });
            }}
            onReview={(request) => {
              setSelectedRequestId(request.request_id);
              inventory.setSelectedDeviceId(request.device_id);
            }}
          />
        </div>

        <div className="workspace-stack">
          <RemoteVerifyCard deployment={deployment} remoteChecklist={remoteChecklist} />
          <TrustSurfaceCard
            accessBusy={accessBusy}
            device={selectedTrustDevice}
            devices={inventory.devices}
            inventory={inventory}
            onTrustAction={setPendingTrustAction}
            onNavigateInventory={() => {
              if (selectedTrustDevice) {
                void navigate(
                  `${getSectionPath("inventory")}?deviceId=${selectedTrustDevice.device_id}`,
                );
              }
            }}
            onNavigateLogs={() => navigate(getSectionPath("logs"))}
          />
          <WorkspaceSectionCard
            description="Published capability metadata still advertises the remaining CLI-only seams so operators know when the browser intentionally stops."
            title="Published handoffs"
          >
            <CapabilityCardList
              emptyMessage="No CLI handoffs are currently published for access."
              entries={groupedCapabilities.cli_handoff}
            />
            <CapabilityCardList
              emptyMessage="No direct dashboard actions are currently published for access."
              entries={groupedCapabilities.direct_action}
            />
          </WorkspaceSectionCard>
        </div>
      </section>

      <WorkspaceConfirmDialog
        confirmLabel={
          pendingDecision?.action === "reject"
            ? "Reject pairing request"
            : "Approve pairing request"
        }
        confirmTone={pendingDecision?.action === "reject" ? "danger" : "accent"}
        description={
          pendingDecision === null
            ? ""
            : pendingDecision.action === "reject"
              ? `Reject ${pendingDecision.request.device_id} and keep the request out of the trusted inventory path.`
              : `Approve ${pendingDecision.request.device_id} and publish its trust material into the paired inventory surface.`
        }
        isBusy={accessBusy}
        isOpen={pendingDecision !== null}
        onConfirm={() => void confirmDecision()}
        onOpenChange={(isOpen) => !isOpen && setPendingDecision(null)}
        title={
          pendingDecision?.action === "reject"
            ? "Reject pairing request"
            : "Approve pairing request"
        }
      />

      <WorkspaceConfirmDialog
        confirmLabel={trustActionLabel(pendingTrustAction)}
        description={describeTrustAction(pendingTrustAction, selectedTrustDevice)}
        isBusy={accessBusy}
        isOpen={pendingTrustAction !== null}
        onConfirm={() => void confirmTrustAction()}
        onOpenChange={(isOpen) => !isOpen && setPendingTrustAction(null)}
        title={trustActionLabel(pendingTrustAction)}
      />
    </main>
  );
}

function readCapabilityCatalog(value: JsonObject | null): CapabilityCatalog | null {
  return value !== null && Array.isArray(value.capabilities)
    ? (value as unknown as CapabilityCatalog)
    : null;
}

function PairingWizardCard({
  app,
  accessBusy,
}: {
  app: AccessSectionProps["app"];
  accessBusy: boolean;
}) {
  const navigate = useNavigate();

  return (
    <WorkspaceSectionCard
      description="The normal pairing path stays web-first: mint a bounded code, hand the bootstrap command to the client, then review trust material before approval."
      title="Guided pairing"
    >
      <ol className="console-compact-list">
        <li>Mint a short-lived pairing code with the right method and issuer label.</li>
        <li>Run the bootstrap command on the target node or desktop companion.</li>
        <li>Approve only after the fingerprint and transcript match the expected device.</li>
        <li>Verify the resulting device appears with the expected trust state in inventory.</li>
      </ol>

      <div className="workspace-form-grid">
        <SelectField
          label="Method"
          options={[
            { key: "pin", label: "PIN code" },
            { key: "qr", label: "QR / companion handoff" },
          ]}
          value={app.supportNodePairingMethod}
          onChange={(value) => app.setSupportNodePairingMethod(value as NodePairingMethod)}
        />
        <TextInputField
          label="Issued by"
          value={app.supportPairingIssuedBy}
          onChange={app.setSupportPairingIssuedBy}
        />
        <TextInputField
          label="TTL ms"
          value={app.supportPairingTtlMs}
          onChange={app.setSupportPairingTtlMs}
        />
      </div>

      <div className="workspace-inline">
        <ActionButton
          isDisabled={accessBusy}
          type="button"
          variant="primary"
          onPress={() => void app.mintSupportPairingCode()}
        >
          {app.supportBusy ? "Minting..." : "Mint node pairing code"}
        </ActionButton>
        <ActionButton type="button" variant="secondary" onPress={() => app.setSection("approvals")}>
          Open approvals
        </ActionButton>
        <ActionButton
          type="button"
          variant="ghost"
          onPress={() => void navigate(getSectionPath("inventory"))}
        >
          Open inventory
        </ActionButton>
      </div>

      {app.supportNodePairingCodes.length === 0 ? (
        <WorkspaceEmptyState
          compact
          description="Mint a bounded code before handing pairing off to a node or desktop companion."
          title="No active node pairing codes"
        />
      ) : (
        <WorkspaceTable
          ariaLabel="Node pairing codes"
          columns={["Code", "Method", "Expires", "Bootstrap handoff"]}
        >
          {app.supportNodePairingCodes.map((code) => (
            <tr key={`${code.code}-${code.created_at_unix_ms}`}>
              <td>
                <div className="workspace-stack">
                  <strong>{code.code}</strong>
                  <small className="text-muted">{code.issued_by || "unknown issuer"}</small>
                </div>
              </td>
              <td>{code.method}</td>
              <td>{formatUnixMs(code.expires_at_unix_ms)}</td>
              <td>
                <code>{buildNodeInstallCommand(code)}</code>
              </td>
            </tr>
          ))}
        </WorkspaceTable>
      )}
    </WorkspaceSectionCard>
  );
}

function PendingApprovalsCard({
  accessBusy,
  app,
  devices,
  requests,
  selectedRequest,
  onApprove,
  onReject,
  onReview,
}: {
  accessBusy: boolean;
  app: AccessSectionProps["app"];
  devices: InventoryDeviceRecord[];
  requests: NodePairingRequestView[];
  selectedRequest: NodePairingRequestView | null;
  onApprove: (request: NodePairingRequestView) => void;
  onReject: (request: NodePairingRequestView) => void;
  onReview: (request: NodePairingRequestView) => void;
}) {
  return (
    <WorkspaceSectionCard
      description="Approval stays tied to concrete trust material: fingerprint, transcript hash, scope, expiry, and the related device posture."
      title="Pending approvals"
    >
      {requests.length === 0 ? (
        <WorkspaceEmptyState
          compact
          description="No node pairing requests have been published yet."
          title="No pairing requests"
        />
      ) : (
        <div className="workspace-stack">
          <WorkspaceTable
            ariaLabel="Node pairing requests"
            columns={["Device", "State", "Trust material", "Expiry", "Action"]}
          >
            {requests.map((request) => {
              const relatedDevice = devices.find(
                (record) => record.device_id === request.device_id,
              );
              const pending = request.state === "pending_approval";
              return (
                <tr key={request.request_id}>
                  <td>
                    <div className="workspace-stack">
                      <strong>{request.device_id}</strong>
                      <small className="text-muted">
                        {request.client_kind} · session {request.session_id}
                      </small>
                    </div>
                  </td>
                  <td>
                    <div className="workspace-stack">
                      <WorkspaceStatusChip tone={workspaceToneForState(request.state)}>
                        {request.state}
                      </WorkspaceStatusChip>
                      <small className="text-muted">
                        {describePairingState(request, relatedDevice)}
                      </small>
                    </div>
                  </td>
                  <td>
                    <div className="workspace-stack">
                      <code>{shortHash(request.identity_fingerprint)}</code>
                      <small className="text-muted">
                        transcript {shortHash(request.transcript_hash_hex)}
                      </small>
                    </div>
                  </td>
                  <td>{formatUnixMs(request.expires_at_unix_ms)}</td>
                  <td>
                    <div className="workspace-inline">
                      <ActionButton type="button" variant="ghost" onPress={() => onReview(request)}>
                        Review trust
                      </ActionButton>
                      {pending && (
                        <>
                          <ActionButton
                            isDisabled={accessBusy}
                            type="button"
                            variant="primary"
                            onPress={() => onApprove(request)}
                          >
                            Approve
                          </ActionButton>
                          <ActionButton
                            isDisabled={accessBusy}
                            type="button"
                            variant="danger"
                            onPress={() => onReject(request)}
                          >
                            Reject
                          </ActionButton>
                        </>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </WorkspaceTable>

          {selectedRequest && (
            <WorkspaceInlineNotice
              title={`Trust review for ${selectedRequest.device_id}`}
              tone={workspaceToneForState(selectedRequest.state)}
            >
              <p>{describeApprovalConsequence(selectedRequest, devices)}</p>
              <p>
                Scope:{" "}
                {selectedRequest.decision_scope_ttl_ms === undefined
                  ? "persistent device trust once approved"
                  : `${formatDuration(selectedRequest.decision_scope_ttl_ms)} decision window`}
                . Certificate expiry: {formatUnixMs(selectedRequest.cert_expires_at_unix_ms)}.
              </p>
              <p>
                Identity fingerprint <code>{selectedRequest.identity_fingerprint}</code>
              </p>
              <p>
                Transcript hash <code>{selectedRequest.transcript_hash_hex}</code>
              </p>
              <TextInputField
                label="Approval / rejection reason"
                value={app.supportPairingDecisionReason}
                onChange={app.setSupportPairingDecisionReason}
              />
            </WorkspaceInlineNotice>
          )}
        </div>
      )}
    </WorkspaceSectionCard>
  );
}

function RemoteVerifyCard({
  deployment,
  remoteChecklist,
}: {
  deployment: DeploymentPostureSummary | null;
  remoteChecklist: Array<{
    label: string;
    status: string;
    detail: string;
    tone: "default" | "warning" | "danger" | "success" | "accent";
  }>;
}) {
  const trustWarning =
    deployment?.remote_bind_detected && !deployment?.tls.gateway_enabled
      ? "Remote bind is visible without gateway TLS. Stop here until TLS is enabled and verified."
      : deployment?.remote_bind_detected
        ? "Remote bind is active. Re-run verify whenever certificates, pins, or gateway CA material rotates."
        : "Remote bind is not currently detected.";
  return (
    <WorkspaceSectionCard
      description="Remote verify stays guided in the browser even when the actual certificate or tunnel proof remains an intentional CLI handoff."
      title="Remote verify checklist"
    >
      <dl className="workspace-key-value-grid">
        <div>
          <dt>Mode</dt>
          <dd>{deployment?.mode ?? "n/a"}</dd>
        </div>
        <div>
          <dt>Bind profile</dt>
          <dd>{deployment?.bind_profile ?? "n/a"}</dd>
        </div>
        <div>
          <dt>Expected admin bind</dt>
          <dd>{deployment?.bind_addresses.admin ?? "n/a"}</dd>
        </div>
        <div>
          <dt>Expected gRPC bind</dt>
          <dd>{deployment?.bind_addresses.grpc ?? "n/a"}</dd>
        </div>
        <div>
          <dt>Last remote access</dt>
          <dd>{formatUnixMs(deployment?.last_remote_admin_access_attempt?.observed_at_unix_ms)}</dd>
        </div>
        <div>
          <dt>Remote fingerprint</dt>
          <dd>{deployment?.last_remote_admin_access_attempt?.remote_ip_fingerprint ?? "n/a"}</dd>
        </div>
      </dl>

      <WorkspaceTable
        ariaLabel="Remote verify checklist"
        columns={["Check", "Status", "Why it matters"]}
      >
        {remoteChecklist.map((item) => (
          <tr key={item.label}>
            <td>{item.label}</td>
            <td>
              <WorkspaceStatusChip tone={item.tone}>{item.status}</WorkspaceStatusChip>
            </td>
            <td>{item.detail}</td>
          </tr>
        ))}
      </WorkspaceTable>

      <WorkspaceInlineNotice
        title="CLI handoffs remain explicit"
        tone={workspaceToneForState(deployment?.remote_bind_detected ? "warning" : "ready")}
      >
        <p>
          Browser UI leads the checklist, but host-local pin verification and SSH topology still
          stay CLI-first on purpose.
        </p>
        <p>
          <code>cargo run -p palyra-cli -- daemon dashboard-url --verify-remote --json</code>
        </p>
        <p>
          <code>
            cargo run -p palyra-cli -- tunnel --ssh &lt;user&gt;@&lt;host&gt; --remote-port 7142
            --local-port 7142
          </code>
        </p>
        <p>
          <code>
            cargo run -p palyra-cli -- support-bundle export --output
            ./artifacts/palyra-support-bundle.zip
          </code>
        </p>
      </WorkspaceInlineNotice>

      <WorkspaceInlineNotice
        title="Trust and recovery"
        tone={workspaceToneForState(deployment?.tls.gateway_enabled ? "ready" : "danger")}
      >
        <p>{trustWarning}</p>
        <p>
          If first-connect still fails after tunnel and pin checks, export a support bundle before
          retrying so recovery has the handshake diagnostics and recent remote-access attempts.
        </p>
      </WorkspaceInlineNotice>
    </WorkspaceSectionCard>
  );
}

function TrustSurfaceCard({
  accessBusy,
  device,
  devices,
  inventory,
  onTrustAction,
  onNavigateInventory,
  onNavigateLogs,
}: {
  accessBusy: boolean;
  device: InventoryDeviceRecord | null;
  devices: InventoryDeviceRecord[];
  inventory: ReturnType<typeof useInventoryDomain>;
  onTrustAction: (action: TrustAction) => void;
  onNavigateInventory: () => void;
  onNavigateLogs: () => void;
}) {
  const fingerprintHistory = Array.isArray(device?.certificate_fingerprint_history)
    ? device.certificate_fingerprint_history
    : [];
  return (
    <WorkspaceSectionCard
      description="Fingerprint history, rotation, revoke, and stale/offline distinction stay visible on the same page as pairing review."
      title="Trust surface"
    >
      {devices.length === 0 ? (
        <WorkspaceEmptyState
          compact
          description="Inventory has not published any devices yet."
          title="No trust records"
        />
      ) : (
        <div className="workspace-stack">
          <SelectField
            label="Device"
            options={devices.map((record) => ({
              key: record.device_id,
              label: `${record.device_id} (${record.trust_state}, ${record.presence_state})`,
            }))}
            value={inventory.selectedDeviceId}
            onChange={inventory.setSelectedDeviceId}
          />

          {device && (
            <>
              <div className="workspace-inline">
                <ActionButton type="button" variant="secondary" onPress={onNavigateInventory}>
                  Open full inventory detail
                </ActionButton>
                <ActionButton type="button" variant="ghost" onPress={onNavigateLogs}>
                  Open logs
                </ActionButton>
              </div>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Presence</dt>
                  <dd>{device.presence_state}</dd>
                </div>
                <div>
                  <dt>Trust</dt>
                  <dd>{device.trust_state}</dd>
                </div>
                <div>
                  <dt>Last seen</dt>
                  <dd>{formatUnixMs(device.last_seen_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>Certificate expiry</dt>
                  <dd>{formatUnixMs(device.current_certificate_expires_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>Identity fingerprint</dt>
                  <dd>
                    <code>{device.identity_fingerprint || "n/a"}</code>
                  </dd>
                </div>
                <div>
                  <dt>Current certificate fingerprint</dt>
                  <dd>
                    <code>{device.current_certificate_fingerprint ?? "n/a"}</code>
                  </dd>
                </div>
              </dl>

              <WorkspaceInlineNotice
                title="State meaning"
                tone={workspaceToneForState(device.trust_state)}
              >
                <p>{describeTrustState(device)}</p>
              </WorkspaceInlineNotice>

              {device.warnings.length > 0 && (
                <WorkspaceInlineNotice title="Warnings" tone="warning">
                  <ul className="console-compact-list">
                    {device.warnings.map((warning) => (
                      <li key={warning}>{warning}</li>
                    ))}
                  </ul>
                </WorkspaceInlineNotice>
              )}

              <TextInputField
                label="Action reason"
                placeholder="operator note for revoke or remove"
                value={inventory.actionReason}
                onChange={inventory.setActionReason}
              />

              <div className="workspace-inline">
                <ActionButton
                  isDisabled={accessBusy || !device.actions.can_rotate}
                  type="button"
                  variant="secondary"
                  onPress={() => onTrustAction("rotate")}
                >
                  Rotate certificate
                </ActionButton>
                <ActionButton
                  isDisabled={accessBusy || !device.actions.can_revoke}
                  type="button"
                  variant="danger"
                  onPress={() => onTrustAction("revoke")}
                >
                  Revoke device
                </ActionButton>
                <ActionButton
                  isDisabled={accessBusy || !device.actions.can_remove}
                  type="button"
                  variant="ghost"
                  onPress={() => onTrustAction("remove")}
                >
                  Remove record
                </ActionButton>
              </div>

              {fingerprintHistory.length === 0 ? (
                <WorkspaceEmptyState
                  compact
                  description="No certificate fingerprint history was published for this device."
                  title="No fingerprint history"
                />
              ) : (
                <WorkspaceTable
                  ariaLabel="Certificate fingerprint history"
                  columns={["Rotation", "Fingerprint"]}
                >
                  {fingerprintHistory.map((fingerprint, index) => (
                    <tr key={`${device.device_id}-${fingerprint}-${index}`}>
                      <td>#{index + 1}</td>
                      <td>
                        <code>{fingerprint}</code>
                      </td>
                    </tr>
                  ))}
                </WorkspaceTable>
              )}
            </>
          )}
        </div>
      )}
    </WorkspaceSectionCard>
  );
}

function buildNodeInstallCommand(code: NodePairingCodeView): string {
  return `cargo run -p palyra-cli -- node install --grpc-url <grpc-url> --gateway-ca-file <gateway-ca-file> --method ${code.method} --pairing-code ${code.code} --start`;
}

function shortHash(value: string): string {
  if (value.trim().length <= 18) return value || "n/a";
  return `${value.slice(0, 10)}...${value.slice(-6)}`;
}

function formatDuration(durationMs: number): string {
  if (durationMs < 60_000) return `${Math.max(1, Math.round(durationMs / 1000))}s`;
  if (durationMs < 3_600_000) return `${Math.max(1, Math.round(durationMs / 60_000))}m`;
  return `${Math.max(1, Math.round(durationMs / 3_600_000))}h`;
}

function describePairingState(
  request: NodePairingRequestView,
  relatedDevice: InventoryDeviceRecord | undefined,
): string {
  if (request.state === "expired") return "Expired before the client finished bootstrap.";
  if (request.state === "rejected") {
    return request.decision_reason?.trim() || "Rejected by operator decision.";
  }
  if (request.state === "approved") {
    return "Approved and waiting for the client to finish certificate handoff.";
  }
  if (request.state === "completed") {
    return "Completed and published into the paired inventory surface.";
  }
  if (relatedDevice && relatedDevice.presence_state !== "ok") {
    return `Pending, but the related device is already ${relatedDevice.presence_state}.`;
  }
  return "Waiting on operator approval.";
}

function describeApprovalConsequence(
  request: NodePairingRequestView,
  devices: InventoryDeviceRecord[],
): string {
  const relatedDevice = devices.find((device) => device.device_id === request.device_id);
  if (request.state === "approved") {
    return "This request is already approved. The client still needs to complete certificate handoff before it becomes a completed paired device.";
  }
  if (request.state === "completed") {
    return "This request already completed. Review the trust surface below for the resulting paired device and certificate history.";
  }
  if (request.state === "rejected") {
    return "This request was rejected and should not be used to bootstrap trust unless a new pairing flow is started.";
  }
  if (request.state === "expired") {
    return "This request expired before it could be approved. Mint a fresh code and restart the client bootstrap path.";
  }
  if (relatedDevice === undefined) {
    return "Approving this request creates a new trusted device record and publishes its identity and certificate material into inventory.";
  }
  return `Approving this request updates trust for ${relatedDevice.device_id}, whose current posture is ${relatedDevice.trust_state} / ${relatedDevice.presence_state}.`;
}

function describeTrustState(device: InventoryDeviceRecord): string {
  if (device.trust_state === "revoked") {
    return "Revoked means the device identity is intentionally blocked and should not be treated as a live peer even if old fingerprints remain visible.";
  }
  if (device.trust_state === "legacy") {
    return "Legacy means the device predates the newer verified trust path and should be reviewed before relying on it for remote access.";
  }
  if (device.presence_state === "offline") {
    return "Offline means trust material exists, but the runtime has not been seen recently enough to treat it as currently reachable.";
  }
  if (device.presence_state === "stale") {
    return "Stale means the device still has trust material, but heartbeat age or pairing backlog suggests verification before approval or remote access.";
  }
  if (device.presence_state === "degraded") {
    return "Degraded means the device is present but missing capability or runtime health expectations.";
  }
  return "Trusted and healthy devices are the baseline for approving new pairings or remote verify handoffs.";
}

function trustActionLabel(action: TrustAction | null): string {
  if (action === "rotate") return "Rotate certificate";
  if (action === "revoke") return "Revoke device";
  if (action === "remove") return "Remove record";
  return "Confirm trust action";
}

function describeTrustAction(
  action: TrustAction | null,
  device: InventoryDeviceRecord | null,
): string {
  if (device === null) return "";
  if (action === "rotate") {
    return `Rotate the active certificate for ${device.device_id} and append a new fingerprint to the trust history.`;
  }
  if (action === "revoke") {
    return `Revoke ${device.device_id} and move it out of the trusted path. This should be used when the device or its certificate can no longer be trusted.`;
  }
  if (action === "remove") {
    return `Remove the inventory record for ${device.device_id}. Use this only after revoke or when cleaning up stale trust state.`;
  }
  return "";
}

function buildRemoteChecklist(
  deployment: DeploymentPostureSummary | null,
  devices: InventoryDeviceRecord[],
): Array<{
  label: string;
  status: string;
  detail: string;
  tone: "default" | "warning" | "danger" | "success" | "accent";
}> {
  const staleDevices = devices.filter((record) => record.presence_state !== "ok").length;
  return [
    {
      label: "TLS gate",
      status: deployment?.tls.gateway_enabled ? "enabled" : "missing",
      detail: "Remote verify should not proceed without gateway TLS enabled.",
      tone: deployment?.tls.gateway_enabled ? "success" : "danger",
    },
    {
      label: "Admin auth",
      status: deployment?.admin_auth_required ? "required" : "unknown",
      detail: "Remote dashboard access should stay behind authenticated operator sessions.",
      tone: deployment?.admin_auth_required ? "success" : "warning",
    },
    {
      label: "Bind profile",
      status: deployment?.bind_profile ?? "unknown",
      detail: "Public or remote bind profiles need explicit verification and acknowledgement.",
      tone:
        deployment?.bind_profile === "loopback"
          ? "default"
          : deployment?.remote_bind_detected
            ? "warning"
            : "accent",
    },
    {
      label: "Remote fingerprint",
      status: deployment?.last_remote_admin_access_attempt?.remote_ip_fingerprint
        ? "captured"
        : "not seen",
      detail:
        "Use the latest remote fingerprint as a hint before accepting new remote access paths.",
      tone: deployment?.last_remote_admin_access_attempt?.remote_ip_fingerprint
        ? "accent"
        : "warning",
    },
    {
      label: "Related nodes",
      status: staleDevices > 0 ? `${staleDevices} stale/offline` : "healthy",
      detail:
        "Stale or offline nodes can make a remote verify flow look broken when the real issue is device reachability.",
      tone: staleDevices > 0 ? "warning" : "success",
    },
  ];
}
