# CLI Parity Acceptance Matrix

Version: `1`

This report is generated from the committed CLI parity matrix plus the current `clap` command tree.
It distinguishes expected parity posture (`done` / `partial` / `intentional_deviation` / `capability_gated`) from validation status against the live CLI surface.

## Summary

- Total entries: `94`
- Verified entries: `94`
- Regression entries: `0`
- Help snapshot coverage: `94` entries

### Expected parity status counts

- `done`: `90`
- `partial`: `4`

### Validation status counts

- `verified`: `94`

## Entries

| Path | Category | Expected | Validation | Snapshot | Aliases | Flags | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `palyra` | `root` | `done` | `verified` | `unix: root-help-unix.txt; windows: root-help-windows.txt` | - | `--profile`, `--config`, `--state-root`, `--log-level`, `--output-format`, `--plain`, `--no-color` | - |
| `version` | `top_level` | `done` | `verified` | `version-help.txt` | - | - | - |
| `setup` | `canonical_family` | `done` | `verified` | `setup-help.txt` | `init` | `--mode`, `--path`, `--force`, `--wizard` | - |
| `doctor` | `top_level` | `done` | `verified` | `doctor-help.txt` | - | `--strict`, `--json` | - |
| `health` | `top_level` | `done` | `verified` | `health-help.txt` | - | `--url`, `--grpc-url` | - |
| `logs` | `top_level` | `done` | `verified` | `logs-help.txt` | - | `--db-path`, `--lines`, `--follow` | - |
| `status` | `top_level` | `done` | `verified` | `status-help.txt` | - | - | - |
| `acp` | `canonical_family` | `done` | `verified` | `acp-help.txt` | - | `--session-key`, `--session-label`, `--require-existing`, `--reset-session` | - |
| `agent` | `top_level` | `done` | `verified` | `agent-help.txt` | - | - | - |
| `agents` | `top_level` | `done` | `verified` | `agents-help.txt` | - | - | - |
| `cron` | `top_level` | `done` | `verified` | `cron-help.txt` | - | - | - |
| `routines` | `top_level` | `done` | `verified` | `routines-help.txt` | - | - | - |
| `memory` | `top_level` | `done` | `verified` | `memory-help.txt` | - | - | - |
| `message` | `top_level` | `done` | `verified` | `message-help.txt` | - | - | - |
| `approvals` | `top_level` | `done` | `verified` | `approvals-help.txt` | - | - | - |
| `sessions` | `top_level` | `done` | `verified` | `sessions-help.txt` | - | - | - |
| `tui` | `top_level` | `done` | `verified` | `tui-help.txt` | - | - | - |
| `auth` | `top_level` | `done` | `verified` | `auth-help.txt` | - | - | - |
| `channels` | `top_level` | `done` | `verified` | `channels-help.txt` | - | - | - |
| `webhooks` | `canonical_family` | `done` | `verified` | `webhooks-help.txt` | - | - | - |
| `docs` | `canonical_family` | `done` | `verified` | `docs-help.txt` | - | - | - |
| `plugins` | `canonical_family` | `done` | `verified` | `plugins-help.txt` | - | - | - |
| `hooks` | `canonical_family` | `done` | `verified` | `hooks-help.txt` | - | - | - |
| `devices` | `top_level` | `done` | `verified` | `devices-help.txt` | - | - | - |
| `node` | `top_level` | `done` | `verified` | `node-help.txt` | - | - | - |
| `nodes` | `top_level` | `done` | `verified` | `nodes-help.txt` | - | - | - |
| `browser` | `canonical_family` | `done` | `verified` | `browser-help.txt` | - | - | - |
| `system` | `canonical_family` | `done` | `verified` | `system-help.txt` | - | - | - |
| `sandbox` | `canonical_family` | `done` | `verified` | `sandbox-help.txt` | - | - | - |
| `completion` | `top_level` | `done` | `verified` | `completion-help.txt` | - | `--shell` | - |
| `onboarding` | `canonical_family` | `done` | `verified` | `onboarding-help.txt` | `onboard` | - | - |
| `configure` | `canonical_family` | `done` | `verified` | `configure-help.txt` | - | - | - |
| `gateway` | `canonical_family` | `done` | `verified` | `gateway-help.txt` | `daemon` | - | - |
| `dashboard` | `canonical_family` | `done` | `verified` | `dashboard-help.txt` | - | - | - |
| `backup` | `canonical_family` | `done` | `verified` | `backup-help.txt` | - | - | - |
| `reset` | `canonical_family` | `done` | `verified` | `reset-help.txt` | - | - | - |
| `uninstall` | `canonical_family` | `done` | `verified` | `uninstall-help.txt` | - | - | - |
| `update` | `canonical_family` | `done` | `verified` | `update-help.txt` | - | - | - |
| `support-bundle` | `top_level` | `done` | `verified` | `support-bundle-help.txt` | - | - | - |
| `policy` | `top_level` | `done` | `verified` | `policy-help.txt` | - | - | - |
| `protocol` | `top_level` | `done` | `verified` | `protocol-help.txt` | - | - | - |
| `config` | `top_level` | `done` | `verified` | `config-help.txt` | - | - | - |
| `models` | `top_level` | `done` | `verified` | `models-help.txt` | - | - | - |
| `patch` | `top_level` | `done` | `verified` | `patch-help.txt` | - | - | - |
| `skills` | `top_level` | `done` | `verified` | `skills-help.txt` | `skill` | - | - |
| `secrets` | `top_level` | `done` | `verified` | `secrets-help.txt` | - | - | - |
| `security` | `top_level` | `done` | `verified` | `security-help.txt` | - | - | - |
| `tunnel` | `top_level` | `done` | `verified` | `tunnel-help.txt` | - | - | - |
| `pairing` | `top_level` | `done` | `verified` | `pairing-help.txt` | - | - | - |
| `acp shim` | `nested_surface` | `done` | `verified` | `acp-shim-help.txt` | - | `--session-id`, `--run-id`, `--prompt`, `--prompt-stdin`, `--ndjson-stdin` | - |
| `auth profiles` | `nested_surface` | `done` | `verified` | `auth-profiles-help.txt` | - | - | - |
| `auth openai` | `nested_surface` | `done` | `verified` | `auth-openai-help.txt` | - | - | - |
| `auth openai api-key` | `nested_surface` | `done` | `verified` | `auth-openai-api-key-help.txt` | - | `--profile-id`, `--api-key-stdin`, `--api-key-prompt`, `--set-default` | - |
| `auth profiles list` | `nested_surface` | `done` | `verified` | `auth-profiles-list-help.txt` | - | - | - |
| `browser session` | `nested_surface` | `done` | `verified` | `browser-session-help.txt` | - | - | - |
| `browser session create` | `nested_surface` | `done` | `verified` | `browser-session-create-help.txt` | - | `--allow-private-targets`, `--allow-downloads`, `--allow-domain`, `--persistence-enabled` | - |
| `browser profiles` | `nested_surface` | `done` | `verified` | `browser-profiles-help.txt` | - | - | - |
| `browser profiles create` | `nested_surface` | `done` | `verified` | `browser-profiles-create-help.txt` | - | `--name`, `--persistence-enabled`, `--private-profile` | - |
| `browser tabs` | `nested_surface` | `done` | `verified` | `browser-tabs-help.txt` | - | - | - |
| `browser tabs open` | `nested_surface` | `done` | `verified` | `browser-tabs-open-help.txt` | - | `--url`, `--activate`, `--allow-private-targets` | - |
| `browser navigate` | `nested_surface` | `done` | `verified` | `browser-navigate-help.txt` | - | `--url`, `--timeout-ms`, `--allow-redirects`, `--allow-private-targets` | - |
| `browser snapshot` | `nested_surface` | `done` | `verified` | `browser-snapshot-help.txt` | - | `--include-dom-snapshot`, `--include-visible-text`, `--output` | - |
| `browser trace` | `nested_surface` | `done` | `verified` | `browser-trace-help.txt` | - | `--output` | - |
| `channels discord` | `nested_surface` | `done` | `verified` | `channels-discord-help.txt` | - | - | - |
| `channels discord setup` | `nested_surface` | `done` | `verified` | `channels-discord-setup-help.txt` | - | `--account-id`, `--verify-channel-id`, `--json` | - |
| `channels discord verify` | `nested_surface` | `done` | `verified` | `channels-discord-verify-help.txt` | `test-send` | `--account-id`, `--to`, `--text`, `--confirm` | - |
| `channels router` | `nested_surface` | `done` | `verified` | `channels-router-help.txt` | - | - | - |
| `channels router preview` | `nested_surface` | `done` | `verified` | `channels-router-preview-help.txt` | - | `--route-channel`, `--text`, `--requested-broadcast` | - |
| `config list` | `nested_surface` | `done` | `verified` | `config-list-help.txt` | `show` | `--path`, `--show-secrets` | - |
| `cron update` | `nested_surface` | `done` | `verified` | `cron-update-help.txt` | `edit` | `--id` | - |
| `cron add` | `nested_surface` | `done` | `verified` | `cron-add-help.txt` | - | `--name`, `--prompt`, `--schedule-type`, `--schedule` | - |
| `cron delete` | `nested_surface` | `done` | `verified` | `cron-delete-help.txt` | `rm` | `--id` | - |
| `cron logs` | `nested_surface` | `done` | `verified` | `cron-logs-help.txt` | `runs` | `--id`, `--limit` | - |
| `routines upsert` | `nested_surface` | `done` | `verified` | `routines-upsert-help.txt` | `apply` | `--name`, `--prompt`, `--trigger-kind` | - |
| `routines create-from-template` | `nested_surface` | `done` | `verified` | `routines-create-from-template-help.txt` | - | `--template-id` | - |
| `routines import` | `nested_surface` | `done` | `verified` | `routines-import-help.txt` | - | - | - |
| `routines logs` | `nested_surface` | `done` | `verified` | `routines-logs-help.txt` | `runs` | `--id`, `--limit` | - |
| `routines delete` | `nested_surface` | `done` | `verified` | `routines-delete-help.txt` | `rm` | `--id` | - |
| `hooks bind` | `nested_surface` | `done` | `verified` | `hooks-bind-help.txt` | `install` | `--event`, `--plugin-id`, `--disabled` | - |
| `memory index` | `nested_surface` | `done` | `verified` | `memory-index-help.txt` | `reindex` | `--batch-size`, `--until-complete`, `--run-maintenance` | - |
| `plugins install` | `nested_surface` | `done` | `verified` | `plugins-install-help.txt` | `bind` | `--artifact`, `--allow-tofu`, `--allow-untrusted` | - |
| `sessions show` | `nested_surface` | `done` | `verified` | `sessions-show-help.txt` | `resume` | `--session-id`, `--session-key`, `--json` | - |
| `skills package` | `nested_surface` | `done` | `verified` | `skills-package-help.txt` | - | - | - |
| `skills package build` | `nested_surface` | `done` | `verified` | `skills-package-build-help.txt` | - | `--manifest`, `--sbom`, `--provenance`, `--output` | - |
| `support-bundle export` | `nested_surface` | `done` | `verified` | `support-bundle-export-help.txt` | - | `--output`, `--max-bytes`, `--journal-hash-limit`, `--error-limit` | - |
| `system event` | `nested_surface` | `done` | `verified` | `system-event-help.txt` | `events` | - | - |
| `node install` | `nested_surface` | `done` | `verified` | `node-install-help.txt` | - | - | - |
| `nodes invoke` | `nested_surface` | `done` | `verified` | `nodes-invoke-help.txt` | - | - | - |
| `onboarding wizard` | `nested_surface` | `done` | `verified` | `onboarding-wizard-help.txt` | - | `--flow`, `--non-interactive`, `--accept-risk` | - |
| `webhooks test` | `nested_surface` | `done` | `verified` | `webhooks-test-help.txt` | - | - | - |
| `browser console` | `placeholder_surface` | `partial` | `verified` | `browser-console-help.txt` | - | `--output` | M42 kept a structured placeholder so the CLI tree stays stable before real console export lands. |
| `browser pdf` | `placeholder_surface` | `partial` | `verified` | `browser-pdf-help.txt` | - | `--output` | The command is discoverable and testable, but the implementation remains intentionally incomplete. |
| `browser select` | `placeholder_surface` | `partial` | `verified` | `browser-select-help.txt` | - | `--selector`, `--value` | - |
| `browser highlight` | `placeholder_surface` | `partial` | `verified` | `browser-highlight-help.txt` | - | `--selector` | - |
