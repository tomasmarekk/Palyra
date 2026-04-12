import { useEffect, useMemo, useState, type FormEvent } from "react";

import type {
  AccessFeatureFlagRecord,
  AccessMembershipView,
  AccessRegistrySnapshot,
  ConsoleApiClient,
} from "../../../consoleApi";
import {
  ActionButton,
  ActionCluster,
  AppForm,
  SelectField,
  TextInputField,
} from "../../components/ui";
import {
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../../components/workspace/WorkspaceChrome";
import { WorkspaceInlineNotice } from "../../components/workspace/WorkspacePatterns";
import { formatUnixMs } from "../../shared";

type AccessControlWorkspaceProps = {
  api: Pick<
    ConsoleApiClient,
    | "getAccessSnapshot"
    | "runAccessBackfill"
    | "setAccessFeatureFlag"
    | "createAccessApiToken"
    | "rotateAccessApiToken"
    | "revokeAccessApiToken"
    | "createAccessWorkspace"
    | "createAccessInvitation"
    | "acceptAccessInvitation"
    | "updateAccessMembershipRole"
    | "removeAccessMembership"
    | "upsertAccessShare"
  >;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
};

const ROLE_OPTIONS = [
  { key: "owner", label: "Owner" },
  { key: "admin", label: "Admin" },
  { key: "operator", label: "Operator" },
] as const;

export function AccessControlWorkspace({ api, setError, setNotice }: AccessControlWorkspaceProps) {
  const [snapshot, setSnapshot] = useState<AccessRegistrySnapshot | null>(null);
  const [busy, setBusy] = useState(false);
  const [lastIssuedToken, setLastIssuedToken] = useState<string | null>(null);
  const [tokenForm, setTokenForm] = useState({
    label: "Compat API",
    principal: "user:operator",
    workspaceId: "",
    role: "operator",
    scopes: "compat.models.read, compat.chat.create, compat.responses.create",
    rateLimitPerMinute: "120",
  });
  const [workspaceForm, setWorkspaceForm] = useState({
    teamName: "Palyra Team",
    workspaceName: "Primary Workspace",
  });
  const [inviteForm, setInviteForm] = useState({
    workspaceId: "",
    invitedIdentity: "",
    role: "operator",
    expiresAtUnixMs: "",
  });
  const [invitationTokenInput, setInvitationTokenInput] = useState("");
  const [shareForm, setShareForm] = useState({
    workspaceId: "",
    resourceKind: "session",
    resourceId: "",
    accessLevel: "read",
  });

  const workspaceOptions = useMemo(
    () =>
      (snapshot?.workspaces ?? []).map((workspace) => ({
        key: workspace.workspace_id,
        label: workspace.display_name,
        description: workspace.workspace_id,
      })),
    [snapshot],
  );

  useEffect(() => {
    void refreshSnapshot();
  }, []);

  async function refreshSnapshot(): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      const response = await api.getAccessSnapshot();
      const normalizedSnapshot = normalizeAccessSnapshot(response.snapshot);
      setSnapshot(normalizedSnapshot);
      if (normalizedSnapshot.workspaces.length > 0) {
        const defaultWorkspaceId = normalizedSnapshot.workspaces[0]?.workspace_id ?? "";
        setTokenForm((current) =>
          current.workspaceId.length > 0
            ? current
            : {
                ...current,
                workspaceId: defaultWorkspaceId,
              },
        );
        setInviteForm((current) =>
          current.workspaceId.length > 0
            ? current
            : {
                ...current,
                workspaceId: defaultWorkspaceId,
              },
        );
        setShareForm((current) =>
          current.workspaceId.length > 0
            ? current
            : {
                ...current,
                workspaceId: defaultWorkspaceId,
              },
        );
      }
    } catch (failure) {
      setError(describeFailure(failure));
    } finally {
      setBusy(false);
    }
  }

  async function toggleFeature(flag: AccessFeatureFlagRecord): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      await api.setAccessFeatureFlag({
        feature_key: flag.key,
        enabled: !flag.enabled,
        stage: flag.stage,
      });
      setNotice(`Feature '${flag.label}' ${flag.enabled ? "disabled" : "enabled"}.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function runBackfill(dryRun: boolean): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      const response = await api.runAccessBackfill({ dry_run: dryRun });
      setSnapshot(response.snapshot);
      setNotice(
        dryRun
          ? `Backfill dry-run completed. ${response.backfill.changed_records} records would change.`
          : `Backfill applied. ${response.backfill.changed_records} records changed.`,
      );
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    } finally {
      setBusy(false);
    }
  }

  async function createToken(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setBusy(true);
    setError(null);
    setLastIssuedToken(null);
    try {
      const response = await api.createAccessApiToken({
        label: tokenForm.label.trim(),
        principal: tokenForm.principal.trim(),
        workspace_id: emptyToUndefined(tokenForm.workspaceId),
        role: tokenForm.role,
        scopes: splitScopes(tokenForm.scopes),
        rate_limit_per_minute: parseNumber(tokenForm.rateLimitPerMinute),
      });
      setLastIssuedToken(response.created.token);
      setNotice(`API token '${response.created.token_record.label}' issued.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function rotateToken(tokenId: string): Promise<void> {
    setBusy(true);
    setError(null);
    setLastIssuedToken(null);
    try {
      const response = await api.rotateAccessApiToken(tokenId);
      setLastIssuedToken(response.rotated.token);
      setNotice(`API token '${response.rotated.token_record.label}' rotated.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function revokeToken(tokenId: string): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      await api.revokeAccessApiToken(tokenId);
      setNotice(`API token '${tokenId}' revoked.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function createWorkspace(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const response = await api.createAccessWorkspace({
        team_name: workspaceForm.teamName.trim(),
        workspace_name: workspaceForm.workspaceName.trim(),
      });
      setNotice(`Workspace '${response.created.workspace.display_name}' created.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function createInvitation(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const response = await api.createAccessInvitation({
        workspace_id: inviteForm.workspaceId,
        invited_identity: inviteForm.invitedIdentity.trim(),
        role: inviteForm.role,
        expires_at_unix_ms:
          parseNumber(inviteForm.expiresAtUnixMs) ?? Date.now() + 7 * 24 * 60 * 60 * 1000,
      });
      setLastIssuedToken(response.created.invitation_token);
      setNotice(`Invitation for '${response.created.invitation.invited_identity}' created.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function acceptInvitation(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await api.acceptAccessInvitation({ invitation_token: invitationTokenInput.trim() });
      setInvitationTokenInput("");
      setNotice("Invitation accepted.");
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function updateMembershipRole(
    membership: AccessMembershipView,
    role: string,
  ): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      await api.updateAccessMembershipRole({
        workspace_id: membership.workspace_id,
        member_principal: membership.principal,
        role,
      });
      setNotice(`Updated ${membership.principal} to ${role}.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function removeMembership(membership: AccessMembershipView): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      await api.removeAccessMembership({
        workspace_id: membership.workspace_id,
        member_principal: membership.principal,
      });
      setNotice(`Removed ${membership.principal} from ${membership.workspace_name}.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  async function upsertShare(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await api.upsertAccessShare({
        workspace_id: shareForm.workspaceId,
        resource_kind: shareForm.resourceKind.trim(),
        resource_id: shareForm.resourceId.trim(),
        access_level: shareForm.accessLevel.trim(),
      });
      setNotice(`Share rule saved for ${shareForm.resourceKind}:${shareForm.resourceId}.`);
      await refreshSnapshot();
    } catch (failure) {
      setError(describeFailure(failure));
      setBusy(false);
    }
  }

  if (snapshot === null) {
    return (
      <WorkspaceSectionCard
        title="Team mode and compat API"
        description="Loading rollout, token, workspace, and sharing controls."
      >
        <WorkspaceStatusChip tone="warning">
          {busy ? "Loading access control" : "No data"}
        </WorkspaceStatusChip>
      </WorkspaceSectionCard>
    );
  }

  return (
    <section className="workspace-stack">
      {lastIssuedToken !== null && (
        <WorkspaceInlineNotice title="New secret material" tone="warning">
          This secret is shown only now. Copy it into a secure client or vault reference before
          refreshing the page.
          <pre className="console-code-block">{lastIssuedToken}</pre>
        </WorkspaceInlineNotice>
      )}

      <WorkspaceSectionCard
        title="Team mode and compat API"
        description="Feature flags keep the external API and shared workspace model opt-in until rollout criteria are met."
      >
        <div className="workspace-stack">
          <WorkspaceInlineNotice
            title="Rollout posture"
            tone={
              snapshot.rollout.external_api_safe_mode && snapshot.rollout.team_mode_safe_mode
                ? "success"
                : "warning"
            }
          >
            External API safe mode: {snapshot.rollout.external_api_safe_mode ? "on" : "off"}. Team
            mode safe mode: {snapshot.rollout.team_mode_safe_mode ? "on" : "off"}.
            {snapshot.rollout.operator_notes.length > 0 && (
              <ul className="workspace-list">
                {snapshot.rollout.operator_notes.map((note) => (
                  <li key={note}>{note}</li>
                ))}
              </ul>
            )}
          </WorkspaceInlineNotice>
        </div>
        <div className="workspace-section-grid">
          {snapshot.feature_flags.map((flag) => (
            <article key={flag.key} className="workspace-surface-card">
              <header className="workspace-surface-card__header">
                <div>
                  <h3>{flag.label}</h3>
                  <p>{flag.description}</p>
                </div>
                <WorkspaceStatusChip tone={flag.enabled ? "success" : "default"}>
                  {flag.enabled ? `Enabled · ${flag.stage}` : `Disabled · ${flag.stage}`}
                </WorkspaceStatusChip>
              </header>
              <ActionCluster>
                <ActionButton
                  isDisabled={busy}
                  type="button"
                  variant={flag.enabled ? "secondary" : "primary"}
                  onPress={() => void toggleFeature(flag)}
                >
                  {flag.enabled ? "Disable" : "Enable"}
                </ActionButton>
              </ActionCluster>
            </article>
          ))}
        </div>
      </WorkspaceSectionCard>

      <WorkspaceSectionCard
        title="API tokens"
        description="Issue scoped compat API credentials, rotate them, and keep the last visible secret short-lived."
      >
        <AppForm onSubmit={(event) => void createToken(event)}>
          <TextInputField
            label="Label"
            value={tokenForm.label}
            onChange={(value) => setTokenForm((current) => ({ ...current, label: value }))}
          />
          <TextInputField
            label="Principal"
            value={tokenForm.principal}
            onChange={(value) => setTokenForm((current) => ({ ...current, principal: value }))}
          />
          <SelectField
            label="Workspace"
            value={tokenForm.workspaceId}
            options={[{ key: "", label: "Personal token" }, ...workspaceOptions]}
            onChange={(value) => setTokenForm((current) => ({ ...current, workspaceId: value }))}
          />
          <SelectField
            label="Role"
            value={tokenForm.role}
            options={ROLE_OPTIONS}
            onChange={(value) => setTokenForm((current) => ({ ...current, role: value }))}
          />
          <TextInputField
            label="Scopes"
            value={tokenForm.scopes}
            description="Comma-separated scopes such as compat.chat.create or workspace.manage."
            onChange={(value) => setTokenForm((current) => ({ ...current, scopes: value }))}
          />
          <TextInputField
            label="Rate limit per minute"
            value={tokenForm.rateLimitPerMinute}
            type="number"
            onChange={(value) =>
              setTokenForm((current) => ({ ...current, rateLimitPerMinute: value }))
            }
          />
          <ActionButton isDisabled={busy} type="submit" variant="primary">
            Create token
          </ActionButton>
        </AppForm>

        <div className="workspace-stack">
          {snapshot.api_tokens.map((token) => (
            <article key={token.token_id} className="workspace-surface-card">
              <header className="workspace-surface-card__header">
                <div>
                  <h3>{token.label}</h3>
                  <p>
                    {token.token_prefix} · {token.principal} · {token.workspace_id ?? "personal"}
                  </p>
                </div>
                <WorkspaceStatusChip tone={token.status === "active" ? "success" : "warning"}>
                  {token.status}
                </WorkspaceStatusChip>
              </header>
              <p className="workspace-surface-card__detail">
                Scopes: {token.scopes.join(", ")} · last used{" "}
                {formatUnixMs(token.last_used_at_unix_ms)}
              </p>
              <ActionCluster>
                <ActionButton
                  isDisabled={busy || token.status !== "active"}
                  type="button"
                  variant="secondary"
                  onPress={() => void rotateToken(token.token_id)}
                >
                  Rotate
                </ActionButton>
                <ActionButton
                  isDisabled={busy || token.status !== "active"}
                  type="button"
                  variant="secondary"
                  onPress={() => void revokeToken(token.token_id)}
                >
                  Revoke
                </ActionButton>
              </ActionCluster>
            </article>
          ))}
        </div>
      </WorkspaceSectionCard>

      <WorkspaceSectionCard
        title="Workspaces, members, and invitations"
        description="Create shared workspaces, invite identities, and keep visible membership boundaries explicit."
      >
        <div className="workspace-two-column-grid">
          <AppForm onSubmit={(event) => void createWorkspace(event)}>
            <TextInputField
              label="Team name"
              value={workspaceForm.teamName}
              onChange={(value) => setWorkspaceForm((current) => ({ ...current, teamName: value }))}
            />
            <TextInputField
              label="Workspace name"
              value={workspaceForm.workspaceName}
              onChange={(value) =>
                setWorkspaceForm((current) => ({ ...current, workspaceName: value }))
              }
            />
            <ActionButton isDisabled={busy} type="submit" variant="primary">
              Create workspace
            </ActionButton>
          </AppForm>

          <AppForm onSubmit={(event) => void createInvitation(event)}>
            <SelectField
              label="Workspace"
              value={inviteForm.workspaceId}
              options={workspaceOptions}
              onChange={(value) => setInviteForm((current) => ({ ...current, workspaceId: value }))}
            />
            <TextInputField
              label="Invited identity"
              value={inviteForm.invitedIdentity}
              onChange={(value) =>
                setInviteForm((current) => ({ ...current, invitedIdentity: value }))
              }
            />
            <SelectField
              label="Role"
              value={inviteForm.role}
              options={ROLE_OPTIONS}
              onChange={(value) => setInviteForm((current) => ({ ...current, role: value }))}
            />
            <TextInputField
              label="Expiry (unix ms)"
              value={inviteForm.expiresAtUnixMs}
              type="number"
              onChange={(value) =>
                setInviteForm((current) => ({ ...current, expiresAtUnixMs: value }))
              }
            />
            <ActionButton isDisabled={busy} type="submit" variant="secondary">
              Create invitation
            </ActionButton>
          </AppForm>

          <AppForm onSubmit={(event) => void acceptInvitation(event)}>
            <TextInputField
              label="Invitation token"
              value={invitationTokenInput}
              onChange={setInvitationTokenInput}
              description="Paste a previously issued invitation token to accept the workspace invite."
            />
            <ActionButton
              isDisabled={busy || invitationTokenInput.trim().length === 0}
              type="submit"
              variant="secondary"
            >
              Accept invitation
            </ActionButton>
          </AppForm>
        </div>

        <div className="workspace-stack">
          {snapshot.memberships.map((membership) => (
            <article
              key={`${membership.workspace_id}:${membership.principal}`}
              className="workspace-surface-card"
            >
              <header className="workspace-surface-card__header">
                <div>
                  <h3>{membership.workspace_name}</h3>
                  <p>
                    {membership.principal} · {membership.role}
                  </p>
                </div>
                <WorkspaceStatusChip tone="accent">{membership.team_name}</WorkspaceStatusChip>
              </header>
              <p className="workspace-surface-card__detail">
                Permissions: {membership.permissions.join(", ")}
              </p>
              <ActionCluster>
                {ROLE_OPTIONS.map((role) => (
                  <ActionButton
                    key={role.key}
                    isDisabled={busy || membership.role === role.key}
                    type="button"
                    variant="secondary"
                    onPress={() => void updateMembershipRole(membership, role.key)}
                  >
                    {role.label}
                  </ActionButton>
                ))}
                <ActionButton
                  isDisabled={busy}
                  type="button"
                  variant="secondary"
                  onPress={() => void removeMembership(membership)}
                >
                  Remove
                </ActionButton>
              </ActionCluster>
            </article>
          ))}
        </div>

        <div className="workspace-stack">
          {snapshot.invitations.map((invitation) => (
            <article key={invitation.invitation_id} className="workspace-surface-card">
              <header className="workspace-surface-card__header">
                <div>
                  <h3>{invitation.invited_identity}</h3>
                  <p>
                    {invitation.role} · expires {formatUnixMs(invitation.expires_at_unix_ms)}
                  </p>
                </div>
                <WorkspaceStatusChip tone={invitation.accepted_at_unix_ms ? "success" : "warning"}>
                  {invitation.accepted_at_unix_ms ? "Accepted" : "Pending"}
                </WorkspaceStatusChip>
              </header>
            </article>
          ))}
        </div>
      </WorkspaceSectionCard>

      <WorkspaceSectionCard
        title="Migration and rollout"
        description="Track phase-10 upgrade readiness, run idempotent backfills, and keep staged rollout packages on an explicit kill-switch contract."
      >
        <WorkspaceInlineNotice
          title="Migration status"
          tone={snapshot.migration.backfill_required ? "warning" : "success"}
        >
          Registry version {snapshot.migration.version} at {snapshot.migration.registry_path}.{" "}
          Backfill required: {snapshot.migration.backfill_required ? "yes" : "no"}. Blocking issues:{" "}
          {snapshot.migration.blocking_issues}. Warnings: {snapshot.migration.warning_issues}. Last
          backfill: {formatUnixMs(snapshot.migration.last_backfill_at_unix_ms)}.
        </WorkspaceInlineNotice>

        <ActionCluster>
          <ActionButton
            isDisabled={busy}
            type="button"
            variant="secondary"
            onPress={() => void runBackfill(true)}
          >
            Dry-run backfill
          </ActionButton>
          <ActionButton
            isDisabled={busy}
            type="button"
            variant="primary"
            onPress={() => void runBackfill(false)}
          >
            Apply backfill
          </ActionButton>
        </ActionCluster>

        <div className="workspace-stack">
          {snapshot.migration.checks.map((check) => (
            <article key={check.key} className="workspace-surface-card">
              <header className="workspace-surface-card__header">
                <div>
                  <h3>{check.key}</h3>
                  <p>{check.detail}</p>
                </div>
                <WorkspaceStatusChip
                  tone={
                    check.state === "ready"
                      ? "success"
                      : check.state === "blocked"
                        ? "danger"
                        : "warning"
                  }
                >
                  {check.state}
                </WorkspaceStatusChip>
              </header>
              <p className="workspace-surface-card__detail">{check.remediation}</p>
            </article>
          ))}
          {snapshot.rollout.packages.map((pkg) => (
            <article key={pkg.feature_key} className="workspace-surface-card">
              <header className="workspace-surface-card__header">
                <div>
                  <h3>{pkg.label}</h3>
                  <p>
                    {pkg.feature_key} · stage {pkg.stage}
                  </p>
                </div>
                <WorkspaceStatusChip tone={pkg.enabled ? "success" : "default"}>
                  {pkg.enabled ? "Enabled" : "Disabled"}
                </WorkspaceStatusChip>
              </header>
              <p className="workspace-surface-card__detail">
                Depends on: {pkg.depends_on.length > 0 ? pkg.depends_on.join(", ") : "none"}.
                Blockers:{" "}
                {pkg.dependency_blockers.length > 0 ? pkg.dependency_blockers.join(", ") : "none"}.
                Kill switch: <code>{pkg.kill_switch_command}</code>
              </p>
            </article>
          ))}
        </div>
      </WorkspaceSectionCard>

      <WorkspaceSectionCard
        title="Sharing and rollout telemetry"
        description="Track explicit share rules and recent rollout/audit signals for the new access surfaces."
      >
        <AppForm onSubmit={(event) => void upsertShare(event)}>
          <SelectField
            label="Workspace"
            value={shareForm.workspaceId}
            options={workspaceOptions}
            onChange={(value) => setShareForm((current) => ({ ...current, workspaceId: value }))}
          />
          <TextInputField
            label="Resource kind"
            value={shareForm.resourceKind}
            onChange={(value) => setShareForm((current) => ({ ...current, resourceKind: value }))}
          />
          <TextInputField
            label="Resource id"
            value={shareForm.resourceId}
            onChange={(value) => setShareForm((current) => ({ ...current, resourceId: value }))}
          />
          <TextInputField
            label="Access level"
            value={shareForm.accessLevel}
            onChange={(value) => setShareForm((current) => ({ ...current, accessLevel: value }))}
          />
          <ActionButton isDisabled={busy} type="submit" variant="secondary">
            Save share
          </ActionButton>
        </AppForm>

        <div className="workspace-stack">
          {snapshot.shares.map((share) => (
            <article key={share.share_id} className="workspace-surface-card">
              <header className="workspace-surface-card__header">
                <div>
                  <h3>
                    {share.resource_kind}:{share.resource_id}
                  </h3>
                  <p>{share.workspace_id}</p>
                </div>
                <WorkspaceStatusChip tone="accent">{share.access_level}</WorkspaceStatusChip>
              </header>
            </article>
          ))}
          {snapshot.telemetry.map((entry) => (
            <article key={entry.feature_key} className="workspace-surface-card">
              <header className="workspace-surface-card__header">
                <div>
                  <h3>{entry.feature_key}</h3>
                  <p>
                    {entry.total_events} events · {entry.success_events} success ·{" "}
                    {entry.error_events} error
                  </p>
                </div>
                <WorkspaceStatusChip tone={entry.error_events > 0 ? "warning" : "success"}>
                  {entry.error_events > 0 ? "Attention" : "Healthy"}
                </WorkspaceStatusChip>
              </header>
              <p className="workspace-surface-card__detail">
                Latest telemetry at {formatUnixMs(entry.latest_at_unix_ms)}
              </p>
            </article>
          ))}
        </div>
      </WorkspaceSectionCard>
    </section>
  );
}

function splitScopes(raw: string): string[] {
  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

function parseNumber(raw: string): number | undefined {
  const trimmed = raw.trim();
  if (trimmed.length === 0) return undefined;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function emptyToUndefined(raw: string): string | undefined {
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function describeFailure(failure: unknown): string {
  return failure instanceof Error ? failure.message : String(failure);
}

function normalizeAccessSnapshot(snapshot: AccessRegistrySnapshot): AccessRegistrySnapshot {
  return {
    ...snapshot,
    feature_flags: Array.isArray(snapshot.feature_flags) ? snapshot.feature_flags : [],
    api_tokens: Array.isArray(snapshot.api_tokens) ? snapshot.api_tokens : [],
    teams: Array.isArray(snapshot.teams) ? snapshot.teams : [],
    workspaces: Array.isArray(snapshot.workspaces) ? snapshot.workspaces : [],
    memberships: Array.isArray(snapshot.memberships) ? snapshot.memberships : [],
    invitations: Array.isArray(snapshot.invitations) ? snapshot.invitations : [],
    shares: Array.isArray(snapshot.shares) ? snapshot.shares : [],
    telemetry: Array.isArray(snapshot.telemetry) ? snapshot.telemetry : [],
    migration: {
      ...snapshot.migration,
      checks: Array.isArray(snapshot.migration?.checks) ? snapshot.migration.checks : [],
    },
    rollout: {
      ...snapshot.rollout,
      packages: Array.isArray(snapshot.rollout?.packages) ? snapshot.rollout.packages : [],
      operator_notes: Array.isArray(snapshot.rollout?.operator_notes)
        ? snapshot.rollout.operator_notes
        : [],
    },
  };
}
