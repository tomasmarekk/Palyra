import { useState } from "react";

import { ActionButton, TextInputField } from "../components/ui";
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
  WorkspaceRedactedValue,
  WorkspaceTable,
} from "../components/workspace/WorkspacePatterns";
import { formatUnixMs, readNumber, readString } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SecretsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "configBusy"
    | "configSecretsScope"
    | "setConfigSecretsScope"
    | "configSecrets"
    | "configSecretKey"
    | "setConfigSecretKey"
    | "configSecretMetadata"
    | "configSecretValue"
    | "setConfigSecretValue"
    | "configSecretReveal"
    | "revealSensitiveValues"
    | "refreshSecrets"
    | "loadSecretMetadata"
    | "setSecretValue"
    | "revealSecretValue"
    | "deleteSecretValue"
  >;
};

export function SecretsSection({ app }: SecretsSectionProps) {
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const selectedScope =
    readString(app.configSecretMetadata ?? {}, "scope") ??
    readString(app.configSecretReveal ?? {}, "scope") ??
    app.configSecretsScope;
  const selectedKeyCandidate =
    readString(app.configSecretMetadata ?? {}, "key") ??
    readString(app.configSecretReveal ?? {}, "key") ??
    app.configSecretKey.trim();
  const selectedKey = selectedKeyCandidate.length > 0 ? selectedKeyCandidate : "No key selected";
  const revealedSecret =
    readString(app.configSecretReveal ?? {}, "value_utf8") ??
    readString(app.configSecretReveal ?? {}, "value_base64");
  const totalBytes = app.configSecrets.reduce(
    (sum, secret) => sum + (readNumber(secret, "value_bytes") ?? 0),
    0,
  );

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Settings"
        title="Secrets"
        description="Manage vault-backed secrets with explicit metadata reads, deliberate reveal actions, and destructive deletion kept behind confirmation."
        status={
          <>
            <WorkspaceStatusChip tone="warning">
              {app.configSecrets.length} secret metadata rows
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={app.configSecretReveal === null ? "default" : "danger"}>
              {app.configSecretReveal === null ? "Values masked" : "Reveal loaded"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            type="button"
            variant="primary"
            onPress={() => void app.refreshSecrets()}
            isDisabled={app.configBusy}
          >
            {app.configBusy ? "Refreshing..." : "Refresh secrets"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Current scope"
          value={app.configSecretsScope || "global"}
          detail="Scope determines which vault namespace is listed and mutated."
        />
        <WorkspaceMetricCard
          label="Visible metadata"
          value={app.configSecrets.length}
          detail="Only metadata is listed by default. Raw secret bytes stay hidden."
          tone={app.configSecrets.length > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label="Approx bytes"
          value={totalBytes}
          detail="Combined metadata only, not revealed secret payload length in memory."
          tone={totalBytes > 0 ? "accent" : "default"}
        />
      </section>

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Secret inventory"
            description="Pick a secret to inspect metadata or reveal it explicitly in the current session."
            actions={
              <TextInputField
                label="Scope"
                value={app.configSecretsScope}
                onChange={app.setConfigSecretsScope}
              />
            }
          >
            {app.configSecrets.length === 0 ? (
              <WorkspaceEmptyState
                title="No secret metadata loaded"
                description="This scope is empty or has not been refreshed yet. Select a scope and refresh the list before operating on a key."
                compact
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Secret metadata"
                columns={["Key", "Scope", "Updated", "Size", "Actions"]}
              >
                {app.configSecrets.map((secret) => {
                  const key = readString(secret, "key") ?? "unknown";
                  return (
                    <tr key={`${readString(secret, "scope") ?? "scope"}-${key}`}>
                      <td>
                        <div className="workspace-table__meta">
                          <strong>{key}</strong>
                          <span className="chat-muted">
                            Created {formatUnixMs(readNumber(secret, "created_at_unix_ms"))}
                          </span>
                        </div>
                      </td>
                      <td>{readString(secret, "scope") ?? app.configSecretsScope}</td>
                      <td>{formatUnixMs(readNumber(secret, "updated_at_unix_ms"))}</td>
                      <td>{readString(secret, "value_bytes") ?? "n/a"}</td>
                      <td>
                        <div className="workspace-table__actions">
                          <ActionButton
                            type="button"
                            variant="secondary"
                            onPress={() => {
                              app.setConfigSecretKey(key);
                              void app.loadSecretMetadata(key);
                            }}
                            isDisabled={app.configBusy}
                          >
                            Inspect
                          </ActionButton>
                          <ActionButton
                            type="button"
                            variant="secondary"
                            onPress={() => {
                              app.setConfigSecretKey(key);
                              void app.revealSecretValue(key);
                            }}
                            isDisabled={app.configBusy}
                          >
                            Reveal
                          </ActionButton>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Loaded secret"
            description="Revealed values remain masked unless you opt into the dashboard-wide reveal toggle."
          >
            <WorkspaceRedactedValue
              label={selectedKey}
              value={revealedSecret}
              sensitive
              revealed={app.revealSensitiveValues}
              onReveal={() => void app.revealSecretValue()}
              allowCopy
              placeholder="No secret has been revealed in this session."
              hint={`Scope: ${selectedScope}`}
            />
          </WorkspaceSectionCard>
        </div>

        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Operate on a secret"
            description="Metadata reads, writes, reveal, and delete all stay explicit on the selected key."
          >
            <div className="workspace-stack">
              <TextInputField
                label="Key"
                value={app.configSecretKey}
                onChange={app.setConfigSecretKey}
              />
              <TextInputField
                label="Value"
                type="password"
                autoComplete="off"
                value={app.configSecretValue}
                onChange={app.setConfigSecretValue}
              />
              <div className="workspace-inline">
                <ActionButton
                  type="button"
                  variant="primary"
                  onPress={() => void app.loadSecretMetadata()}
                  isDisabled={app.configBusy}
                >
                  Load metadata
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="primary"
                  onPress={() => void app.setSecretValue()}
                  isDisabled={app.configBusy}
                >
                  {app.configBusy ? "Working..." : "Store secret"}
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="secondary"
                  onPress={() => void app.revealSecretValue()}
                  isDisabled={app.configBusy}
                >
                  Explicit reveal
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="danger"
                  onPress={() => setConfirmingDelete(true)}
                  isDisabled={app.configBusy || app.configSecretKey.trim().length === 0}
                >
                  Delete secret
                </ActionButton>
              </div>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Selected metadata"
            description="Metadata provides safe context without exposing the stored value."
          >
            {app.configSecretMetadata === null ? (
              <WorkspaceEmptyState
                title="No metadata loaded"
                description="Inspect a key from the table or enter one manually to load created/updated timestamps and stored size."
                compact
              />
            ) : (
              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Scope</dt>
                  <dd>{readString(app.configSecretMetadata, "scope") ?? app.configSecretsScope}</dd>
                </div>
                <div>
                  <dt>Key</dt>
                  <dd>{readString(app.configSecretMetadata, "key") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Created</dt>
                  <dd>
                    {formatUnixMs(readNumber(app.configSecretMetadata, "created_at_unix_ms"))}
                  </dd>
                </div>
                <div>
                  <dt>Updated</dt>
                  <dd>
                    {formatUnixMs(readNumber(app.configSecretMetadata, "updated_at_unix_ms"))}
                  </dd>
                </div>
                <div>
                  <dt>Value bytes</dt>
                  <dd>{readString(app.configSecretMetadata, "value_bytes") ?? "n/a"}</dd>
                </div>
              </dl>
            )}
          </WorkspaceSectionCard>

          <WorkspaceInlineNotice title="Why this page is strict" tone="warning">
            <p>
              Secret values stay redacted by default. Reveal is deliberate, delete requires
              confirmation, and the page avoids mixing config edits with credential storage.
            </p>
          </WorkspaceInlineNotice>
        </div>
      </section>

      <WorkspaceConfirmDialog
        isOpen={confirmingDelete}
        onOpenChange={setConfirmingDelete}
        title="Delete secret"
        description={`Delete ${app.configSecretKey.trim()} from scope ${app.configSecretsScope}? This removes the stored value for future operators and agents.`}
        confirmLabel="Delete secret"
        confirmTone="danger"
        isBusy={app.configBusy}
        onConfirm={() => {
          setConfirmingDelete(false);
          void app.deleteSecretValue();
        }}
      />
    </main>
  );
}
