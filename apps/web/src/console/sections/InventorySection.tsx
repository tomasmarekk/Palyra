import { useNavigate, useSearchParams } from "react-router-dom";

import { getSectionPath } from "../navigation";
import { ActionButton, SelectField, TextAreaField, TextInputField } from "../components/ui";
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
import { useInventoryDomain } from "../hooks/useInventoryDomain";
import { PrettyJsonBlock, formatUnixMs } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type InventorySectionProps = {
  app: Pick<ConsoleAppState, "api" | "setError" | "setNotice" | "revealSensitiveValues">;
};

export function InventorySection({ app }: InventorySectionProps) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const inventory = useInventoryDomain({
    api: app.api,
    preferredDeviceId: searchParams.get("deviceId") ?? undefined,
    setError: app.setError,
    setNotice: app.setNotice,
  });
  const selected = inventory.selectedDevice;
  const attentionCount =
    (inventory.summary?.stale_devices ?? 0) + (inventory.summary?.degraded_devices ?? 0);

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Observability"
        title="Inventory"
        description="Keep nodes, paired devices, active pairing backlog, and live runtime instances in one operator map instead of scattering trust and heartbeat state across unrelated pages."
        status={
          <>
            <WorkspaceStatusChip tone={inventory.busy ? "warning" : "success"}>
              {inventory.busy ? "Refreshing" : "Inventory ready"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={attentionCount > 0 ? "warning" : "default"}>
              {attentionCount} attention states
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={(inventory.summary?.offline_devices ?? 0) > 0 ? "danger" : "default"}
            >
              {inventory.summary?.offline_devices ?? 0} offline devices
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            isDisabled={inventory.busy}
            type="button"
            variant="primary"
            onPress={() => void inventory.refreshInventory(inventory.selectedDeviceId)}
          >
            {inventory.busy ? "Refreshing..." : "Refresh inventory"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail="Devices known to the identity and node runtime inventory."
          label="Devices"
          value={inventory.summary?.devices ?? inventory.devices.length}
        />
        <WorkspaceMetricCard
          detail="Devices with current trust material and approval provenance."
          label="Trusted"
          tone={(inventory.summary?.trusted_devices ?? 0) > 0 ? "success" : "default"}
          value={inventory.summary?.trusted_devices ?? 0}
        />
        <WorkspaceMetricCard
          detail="Pairing requests still waiting on approval or completion."
          label="Pending pairings"
          tone={(inventory.summary?.pending_pairings ?? 0) > 0 ? "warning" : "default"}
          value={inventory.summary?.pending_pairings ?? 0}
        />
        <WorkspaceMetricCard
          detail="Runtime instances currently published by the unified observability contract."
          label="Instances"
          value={inventory.instances.length}
        />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Runtime presence, trust, and capability posture stay sorted by operator attention instead of forcing separate node and device lookups."
          title="Device inventory"
        >
          {inventory.devices.length === 0 ? (
            <WorkspaceEmptyState
              description="Refresh inventory after pairing or runtime activity to populate this view."
              title="No devices published"
            />
          ) : (
            <WorkspaceTable
              ariaLabel="Device inventory"
              columns={["Device", "Presence", "Trust", "Platform", "Pending"]}
            >
              {inventory.devices.map((record) => {
                const selectedRow = record.device_id === inventory.selectedDeviceId;
                return (
                  <tr
                    key={record.device_id}
                    className={selectedRow ? "bg-content2/60" : undefined}
                    onClick={() => inventory.setSelectedDeviceId(record.device_id)}
                  >
                    <td>
                      <div className="workspace-stack">
                        <strong>{record.device_id}</strong>
                        <small className="text-muted">
                          {record.client_kind} · {record.device_status}
                        </small>
                      </div>
                    </td>
                    <td>{record.presence_state}</td>
                    <td>{record.trust_state}</td>
                    <td>{record.platform ?? "n/a"}</td>
                    <td>{record.pending_pairings}</td>
                  </tr>
                );
              })}
            </WorkspaceTable>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Detail and actions stay scoped to one device so rotate, revoke, remove, and test invoke remain explicit and auditable."
          title="Device detail"
        >
          {selected === null ? (
            <WorkspaceEmptyState
              compact
              description="Select a device from the inventory table to inspect heartbeat, pairings, and available actions."
              title="No device selected"
            />
          ) : (
            <div className="workspace-stack">
              <div className="workspace-inline">
                {selected.latest_session_id ? (
                  <ActionButton
                    type="button"
                    variant="secondary"
                    onPress={() =>
                      void navigate(
                        `${getSectionPath("sessions")}?sessionId=${selected.latest_session_id ?? ""}`,
                      )
                    }
                  >
                    Open session
                  </ActionButton>
                ) : null}
                <ActionButton
                  type="button"
                  variant="secondary"
                  onPress={() => void navigate(getSectionPath("logs"))}
                >
                  Open logs
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="ghost"
                  onPress={() => void navigate(getSectionPath("support"))}
                >
                  Open support
                </ActionButton>
              </div>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Presence</dt>
                  <dd>{selected.presence_state}</dd>
                </div>
                <div>
                  <dt>Trust</dt>
                  <dd>{selected.trust_state}</dd>
                </div>
                <div>
                  <dt>Last seen</dt>
                  <dd>{formatUnixMs(selected.last_seen_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>Heartbeat age</dt>
                  <dd>
                    {selected.heartbeat_age_ms === undefined
                      ? "n/a"
                      : `${Math.round(selected.heartbeat_age_ms / 1000)}s`}
                  </dd>
                </div>
                <div>
                  <dt>Capabilities</dt>
                  <dd>
                    {selected.capability_summary.available}/{selected.capability_summary.total}
                  </dd>
                </div>
                <div>
                  <dt>Certificate expiry</dt>
                  <dd>{formatUnixMs(selected.current_certificate_expires_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>Identity fingerprint</dt>
                  <dd>
                    <code>{selected.identity_fingerprint || "n/a"}</code>
                  </dd>
                </div>
                <div>
                  <dt>Current cert fingerprint</dt>
                  <dd>
                    <code>{selected.current_certificate_fingerprint ?? "n/a"}</code>
                  </dd>
                </div>
              </dl>

              {inventory.selectedDetail?.workspace_activity ? (
                <WorkspaceSectionCard
                  description="Checkpoint provenance and restore outcomes stay attached to the device detail so distributed rollback activity remains traceable."
                  title="Workspace restore activity"
                >
                  <div className="workspace-stack">
                    <dl className="workspace-key-value-grid">
                      <div>
                        <dt>Checkpoints</dt>
                        <dd>{inventory.selectedDetail.workspace_activity.summary.checkpoint_count}</dd>
                      </div>
                      <div>
                        <dt>Restore reports</dt>
                        <dd>
                          {inventory.selectedDetail.workspace_activity.summary.restore_report_count}
                        </dd>
                      </div>
                      <div>
                        <dt>Successful restores</dt>
                        <dd>
                          {
                            inventory.selectedDetail.workspace_activity.summary
                              .succeeded_restore_count
                          }
                        </dd>
                      </div>
                      <div>
                        <dt>Partial failures</dt>
                        <dd>
                          {
                            inventory.selectedDetail.workspace_activity.summary
                              .partial_failure_restore_count
                          }
                        </dd>
                      </div>
                      <div>
                        <dt>Failed restores</dt>
                        <dd>
                          {inventory.selectedDetail.workspace_activity.summary.failed_restore_count}
                        </dd>
                      </div>
                      <div>
                        <dt>Total restore attempts</dt>
                        <dd>
                          {
                            inventory.selectedDetail.workspace_activity.summary
                              .checkpoint_restore_total
                          }
                        </dd>
                      </div>
                    </dl>

                    {inventory.selectedDetail.workspace_activity.recent_checkpoints.length > 0 ? (
                      <WorkspaceTable
                        ariaLabel="Recent workspace checkpoints"
                        columns={["Checkpoint", "Created", "Source", "Restores"]}
                      >
                        {inventory.selectedDetail.workspace_activity.recent_checkpoints.map(
                          (checkpoint) => (
                            <tr key={checkpoint.checkpoint_id}>
                              <td>
                                <div className="workspace-stack">
                                  <strong>{checkpoint.source_label}</strong>
                                  <small className="text-muted">{checkpoint.checkpoint_id}</small>
                                </div>
                              </td>
                              <td>{formatUnixMs(checkpoint.created_at_unix_ms)}</td>
                              <td>{checkpoint.tool_name ?? checkpoint.source_kind}</td>
                              <td>{checkpoint.restore_count}</td>
                            </tr>
                          ),
                        )}
                      </WorkspaceTable>
                    ) : (
                      <WorkspaceEmptyState
                        compact
                        description="Workspace checkpoints will appear here after mutating tool activity creates rollback points on this device."
                        title="No workspace checkpoints"
                      />
                    )}

                    {inventory.selectedDetail.workspace_activity.recent_restore_reports.length > 0 ? (
                      <WorkspaceTable
                        ariaLabel="Recent workspace restore reports"
                        columns={["Report", "Created", "Result", "Scope"]}
                      >
                        {inventory.selectedDetail.workspace_activity.recent_restore_reports.map(
                          (report) => (
                            <tr key={report.report_id}>
                              <td>
                                <div className="workspace-stack">
                                  <strong>{report.result_state}</strong>
                                  <small className="text-muted">{report.report_id}</small>
                                </div>
                              </td>
                              <td>{formatUnixMs(report.created_at_unix_ms)}</td>
                              <td>{report.reconciliation_summary}</td>
                              <td>{report.scope_kind}</td>
                            </tr>
                          ),
                        )}
                      </WorkspaceTable>
                    ) : null}
                  </div>
                </WorkspaceSectionCard>
              ) : null}

              {selected.capabilities.length > 0 ? (
                <WorkspaceSectionCard
                  description="Capability inventory includes the execution posture published for this device so handoff semantics stay explicit."
                  title="Capability inventory"
                >
                  <WorkspaceTable
                    ariaLabel="Device capabilities"
                    columns={["Capability", "Available", "Execution mode"]}
                  >
                    {selected.capabilities.map((capability) => (
                      <tr key={capability.name}>
                        <td>{capability.name}</td>
                        <td>{capability.available ? "yes" : "no"}</td>
                        <td>{capability.execution_mode}</td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                </WorkspaceSectionCard>
              ) : null}

              {selected.warnings.length > 0 ? (
                <WorkspaceInlineNotice title="Warnings" tone="warning">
                  <ul className="console-compact-list">
                    {selected.warnings.map((warning) => (
                      <li key={warning}>{warning}</li>
                    ))}
                  </ul>
                </WorkspaceInlineNotice>
              ) : null}

              <WorkspaceInlineNotice
                title="Action posture"
                tone={workspaceToneForState(selected.presence_state)}
              >
                <p>
                  Rotate, revoke, remove, and invoke are still backed by the daemon contracts; this
                  page only unifies the read model and action entry point.
                </p>
              </WorkspaceInlineNotice>

              <div className="workspace-form-grid">
                <TextInputField
                  label="Action reason"
                  placeholder="operator note for revoke or remove"
                  value={inventory.actionReason}
                  onChange={inventory.setActionReason}
                />
                <SelectField
                  label="Capability"
                  options={[
                    { key: "", label: "Select capability" },
                    ...selected.capabilities
                      .filter((entry) => entry.available)
                      .map((entry) => ({ key: entry.name, label: entry.name })),
                  ]}
                  value={inventory.invokeCapability}
                  onChange={inventory.setInvokeCapability}
                />
                <TextAreaField
                  label="Invoke JSON"
                  rows={6}
                  value={inventory.invokeInputJson}
                  onChange={inventory.setInvokeInputJson}
                />
              </div>

              <div className="workspace-inline">
                <ActionButton
                  isDisabled={inventory.busy || !selected.actions.can_rotate}
                  type="button"
                  variant="secondary"
                  onPress={() => void inventory.rotateSelectedDevice()}
                >
                  Rotate
                </ActionButton>
                <ActionButton
                  isDisabled={inventory.busy || !selected.actions.can_revoke}
                  type="button"
                  variant="danger"
                  onPress={() => void inventory.revokeSelectedDevice()}
                >
                  Revoke
                </ActionButton>
                <ActionButton
                  isDisabled={inventory.busy || !selected.actions.can_remove}
                  type="button"
                  variant="ghost"
                  onPress={() => void inventory.removeSelectedDevice()}
                >
                  Remove
                </ActionButton>
                <ActionButton
                  isDisabled={
                    inventory.busy ||
                    !selected.actions.can_invoke ||
                    inventory.invokeCapability.trim().length === 0
                  }
                  type="button"
                  variant="primary"
                  onPress={() => void inventory.invokeSelectedNode()}
                >
                  Invoke capability
                </ActionButton>
              </div>

              {inventory.invokeResult === null ? null : (
                <div className="workspace-stack">
                  <WorkspaceStatusChip tone={inventory.invokeResult.success ? "success" : "danger"}>
                    {inventory.invokeResult.success ? "Invoke succeeded" : "Invoke failed"}
                  </WorkspaceStatusChip>
                  <PrettyJsonBlock
                    revealSensitiveValues={app.revealSensitiveValues}
                    value={
                      inventory.invokeResult.output_json ?? {
                        device_id: inventory.invokeResult.device_id,
                        capability: inventory.invokeResult.capability,
                        success: inventory.invokeResult.success,
                        error: inventory.invokeResult.error,
                      }
                    }
                  />
                </div>
              )}

              <WorkspaceSectionCard
                description="Pairing history stays visible next to the device instead of only in approval-oriented surfaces."
                title="Pairings"
              >
                {inventory.selectedPairings.length === 0 ? (
                  <WorkspaceEmptyState
                    compact
                    description="This device does not currently publish node pairing history."
                    title="No pairings"
                  />
                ) : (
                  <WorkspaceTable
                    ariaLabel="Device pairing history"
                    columns={["Requested", "State", "Session", "Fingerprint"]}
                  >
                    {inventory.selectedPairings.map((record) => (
                      <tr key={record.request_id}>
                        <td>{formatUnixMs(record.requested_at_unix_ms)}</td>
                        <td>{record.state}</td>
                        <td>{record.session_id}</td>
                        <td>
                          <code>{record.identity_fingerprint || "n/a"}</code>
                        </td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                )}
              </WorkspaceSectionCard>

              <WorkspaceSectionCard
                description="Capability handoff stays visible next to the device so operators can distinguish queued work, local mediation, and completed outcomes."
                title="Capability requests"
              >
                {inventory.selectedDetail?.capability_requests.length ? (
                  <WorkspaceTable
                    ariaLabel="Device capability request history"
                    columns={["Created", "Capability", "State", "Input", "Outcome"]}
                  >
                    {inventory.selectedDetail.capability_requests.map((record) => (
                      <tr key={record.request_id}>
                        <td>{formatUnixMs(record.created_at_unix_ms)}</td>
                        <td>
                          <div className="workspace-stack">
                            <strong>{record.capability}</strong>
                            <small className="text-muted">{record.request_id}</small>
                          </div>
                        </td>
                        <td>{record.state}</td>
                        <td>{record.input_summary ?? "n/a"}</td>
                        <td>{record.error ?? record.output_summary ?? "pending"}</td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                ) : (
                  <WorkspaceEmptyState
                    compact
                    description="Capability request history will appear here after the daemon hands work off to this device."
                    title="No capability requests"
                  />
                )}
              </WorkspaceSectionCard>
            </div>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Pending node pairings remain first-class even before the identity becomes a fully paired device record."
          title="Pending pairings"
        >
          {inventory.pendingPairings.length === 0 ? (
            <WorkspaceEmptyState
              compact
              description="No node pairing requests currently require operator attention."
              title="No pending pairings"
            />
          ) : (
            <WorkspaceTable
              ariaLabel="Pending node pairings"
              columns={["Device", "Requested", "State", "Session", "Action"]}
            >
              {inventory.pendingPairings.map((record) => (
                <tr key={record.request_id}>
                  <td>{record.device_id}</td>
                  <td>{formatUnixMs(record.requested_at_unix_ms)}</td>
                  <td>{record.state}</td>
                  <td>{record.session_id}</td>
                  <td>
                    <ActionButton
                      type="button"
                      variant="ghost"
                      onPress={() => void navigate(`${getSectionPath("approvals")}`)}
                    >
                      Open approvals
                    </ActionButton>
                  </td>
                </tr>
              ))}
            </WorkspaceTable>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Operator-visible runtime instances stay next to the node/device map so stale local services do not look like remote node failures."
          title="Runtime instances"
        >
          {inventory.instances.length === 0 ? (
            <WorkspaceEmptyState
              compact
              description="No runtime instances were published by the inventory contract."
              title="No instances"
            />
          ) : (
            <WorkspaceTable
              ariaLabel="Runtime instances"
              columns={["Instance", "Presence", "Observed", "Detail"]}
            >
              {inventory.instances.map((record) => (
                <tr key={record.instance_id}>
                  <td>
                    <div className="workspace-stack">
                      <strong>{record.label}</strong>
                      <small className="text-muted">{record.kind}</small>
                    </div>
                  </td>
                  <td>{record.presence_state}</td>
                  <td>{formatUnixMs(record.observed_at_unix_ms)}</td>
                  <td>{record.detail ?? record.state_label}</td>
                </tr>
              ))}
            </WorkspaceTable>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}
