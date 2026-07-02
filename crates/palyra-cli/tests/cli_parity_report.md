# CLI Parity Acceptance Matrix

Version: `1`

This report is generated from the committed CLI parity matrix plus the current `clap` command tree.
It distinguishes expected parity posture (`done` / `partial` / `intentional_deviation` / `capability_gated`) from validation status against the live CLI surface.

## Summary

- Total entries: `153`
- Verified entries: `153`
- Regression entries: `0`
- Help snapshot coverage: `153` entries

### Expected parity status counts

- `done`: `149`
- `partial`: `4`

### Validation status counts

- `verified`: `153`

## Entries

| Path | Category | Expected | Validation | Snapshot | Aliases | Flags | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `palyra` | `root` | `done` | `verified` | `unix: root-help-unix.txt; windows: root-help-windows.txt` | - | `--profile`, `--config`, `--state-root`, `--log-level`, `--output-format`, `--plain`, `--no-color` | - |
| `version` | `top_level` | `done` | `verified` | `version-help.txt` | - | - | - |
| `setup` | `canonical_family` | `done` | `verified` | `setup-help.txt` | `init` | `--mode`, `--path`, `--force`, `--wizard` | - |
| `doctor` | `top_level` | `done` | `verified` | `doctor-help.txt` | - | `--strict`, `--json`, `--repair`, `--dry-run`, `--force`, `--only`, `--skip`, `--rollback-run` | - |
| `health` | `top_level` | `done` | `verified` | `health-help.txt` | - | `--url`, `--grpc-url` | - |
| `logs` | `top_level` | `done` | `verified` | `logs-help.txt` | - | `--db-path`, `--lines`, `--follow` | - |
| `status` | `top_level` | `done` | `verified` | `status-help.txt` | - | - | - |
| `acp` | `canonical_family` | `done` | `verified` | `acp-help.txt` | - | `--session-key`, `--session-label`, `--require-existing`, `--reset-session` | - |
| `mcp` | `top_level` | `done` | `verified` | `mcp-help.txt` | - | - | - |
| `mcp serve` | `nested_surface` | `done` | `verified` | `mcp-serve-help.txt` | - | `--read-only`, `--allow-sensitive-tools`, `--session-key`, `--session-label` | - |
| `agent` | `top_level` | `done` | `verified` | `agent-help.txt` | - | - | - |
| `agents` | `top_level` | `done` | `verified` | `agents-help.txt` | - | - | - |
| `cron` | `top_level` | `done` | `verified` | `cron-help.txt` | - | - | - |
| `routines` | `top_level` | `done` | `verified` | `routines-help.txt` | - | - | - |
| `objectives` | `top_level` | `done` | `verified` | `objectives-help.txt` | - | - | - |
| `flows` | `top_level` | `done` | `verified` | `flows-help.txt` | - | - | - |
| `tasks` | `top_level` | `done` | `verified` | `tasks-help.txt` | - | - | - |
| `tasks workboard` | `nested_surface` | `done` | `verified` | `tasks-workboard-help.txt` | - | - | - |
| `commitments` | `top_level` | `done` | `verified` | `commitments-help.txt` | - | - | - |
| `memory` | `top_level` | `done` | `verified` | `memory-help.txt` | - | - | - |
| `memory learning` | `nested_surface` | `done` | `verified` | `memory-learning-help.txt` | - | - | - |
| `memory learning list` | `nested_surface` | `done` | `verified` | `memory-learning-list-help.txt` | - | - | - |
| `memory learning review` | `nested_surface` | `done` | `verified` | `memory-learning-review-help.txt` | - | - | - |
| `memory learning apply` | `nested_surface` | `done` | `verified` | `memory-learning-apply-help.txt` | - | - | - |
| `memory learning promote-procedure` | `nested_surface` | `done` | `verified` | `memory-learning-promote-procedure-help.txt` | - | - | - |
| `message` | `top_level` | `done` | `verified` | `message-help.txt` | - | - | - |
| `message read` | `nested_surface` | `done` | `verified` | `message-read-help.txt` | - | `--conversation-id`, `--message-id`, `--before-message-id`, `--after-message-id`, `--around-message-id`, `--limit` | - |
| `message search` | `nested_surface` | `done` | `verified` | `message-search-help.txt` | - | `--conversation-id`, `--query`, `--author-id`, `--has-attachments`, `--before-message-id`, `--limit` | - |
| `message edit` | `nested_surface` | `done` | `verified` | `message-edit-help.txt` | - | `--conversation-id`, `--message-id`, `--text`, `--approval-id` | - |
| `message delete` | `nested_surface` | `done` | `verified` | `message-delete-help.txt` | - | `--conversation-id`, `--message-id`, `--reason`, `--approval-id` | - |
| `message react` | `nested_surface` | `done` | `verified` | `message-react-help.txt` | - | `--conversation-id`, `--message-id`, `--emoji`, `--remove`, `--approval-id` | - |
| `approvals` | `top_level` | `done` | `verified` | `approvals-help.txt` | - | - | - |
| `sessions` | `top_level` | `done` | `verified` | `sessions-help.txt` | - | - | - |
| `tui` | `top_level` | `done` | `verified` | `tui-help.txt` | - | - | - |
| `auth` | `top_level` | `done` | `verified` | `auth-help.txt` | - | - | - |
| `channels` | `top_level` | `done` | `verified` | `channels-help.txt` | - | - | - |
| `webhooks` | `canonical_family` | `done` | `verified` | `webhooks-help.txt` | - | - | - |
| `docs` | `canonical_family` | `done` | `verified` | `docs-help.txt` | - | - | - |
| `plugins` | `canonical_family` | `done` | `verified` | `plugins-help.txt` | - | - | - |
| `hooks` | `canonical_family` | `done` | `verified` | `hooks-help.txt` | - | - | - |
| `profile` | `canonical_family` | `done` | `verified` | `profile-help.txt` | - | - | - |
| `devices` | `top_level` | `done` | `verified` | `devices-help.txt` | - | - | - |
| `node` | `top_level` | `done` | `verified` | `node-help.txt` | - | - | - |
| `nodes` | `top_level` | `done` | `verified` | `nodes-help.txt` | - | - | - |
| `browser` | `canonical_family` | `done` | `verified` | `browser-help.txt` | - | - | - |
| `browser open` | `nested_surface` | `done` | `verified` | `browser-open-help.txt` | - | `--url`, `--principal`, `--channel`, `--allow-private-targets`, `--allow-downloads`, `--profile-id`, `--private-profile`, `--timeout-ms` | - |
| `system` | `canonical_family` | `done` | `verified` | `system-help.txt` | - | - | - |
| `state` | `canonical_family` | `done` | `verified` | `state-help.txt` | - | - | - |
| `state doctor` | `nested_surface` | `done` | `verified` | `state-doctor-help.txt` | - | `--db-path`, `--fast-window`, `--full`, `--json` | - |
| `state verify-hash-chain` | `nested_surface` | `done` | `verified` | `state-verify-hash-chain-help.txt` | - | `--db-path`, `--full`, `--limit`, `--json` | - |
| `state repair` | `nested_surface` | `done` | `verified` | `state-repair-help.txt` | - | `--db-path`, `--dry-run`, `--fts-only`, `--actor-principal`, `--json` | - |
| `state checkpoint` | `nested_surface` | `done` | `verified` | `state-checkpoint-help.txt` | - | `--db-path`, `--mode`, `--json` | - |
| `state sidecars-prepare` | `nested_surface` | `done` | `verified` | `state-sidecars-prepare-help.txt` | - | `--db-path`, `--json` | - |
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
| `eval` | `top_level` | `done` | `verified` | `eval-help.txt` | - | - | - |
| `eval bundle` | `nested_surface` | `done` | `verified` | `eval-bundle-help.txt` | - | - | - |
| `eval bundle create` | `nested_surface` | `done` | `verified` | `eval-bundle-create-help.txt` | - | `--name`, `--output`, `--run-id`, `--run-export`, `--replay-bundle`, `--scenario-manifest`, `--memory-fixture`, `--journal-db`, `--max-events`, `--fake-provider`, `--json` | - |
| `qa` | `top_level` | `done` | `verified` | `qa-help.txt` | - | - | - |
| `qa validate` | `nested_surface` | `done` | `verified` | `qa-validate-help.txt` | - | `--path`, `--json` | - |
| `config` | `top_level` | `done` | `verified` | `config-help.txt` | - | - | - |
| `models` | `top_level` | `done` | `verified` | `models-help.txt` | - | - | - |
| `patch` | `top_level` | `done` | `verified` | `patch-help.txt` | - | - | - |
| `patch bundles` | `nested_surface` | `done` | `verified` | `patch-bundles-help.txt` | - | - | - |
| `workers` | `canonical_family` | `done` | `verified` | `workers-help.txt` | - | - | - |
| `run` | `top_level` | `done` | `verified` | `run-help.txt` | - | - | - |
| `run export` | `nested_surface` | `done` | `verified` | `run-export-help.txt` | - | `--run-id`, `--output`, `--format`, `--redacted`, `--journal-db`, `--max-events` | - |
| `skills` | `top_level` | `done` | `verified` | `skills-help.txt` | `skill` | - | - |
| `secrets` | `top_level` | `done` | `verified` | `secrets-help.txt` | - | - | - |
| `security` | `top_level` | `done` | `verified` | `security-help.txt` | - | - | - |
| `tunnel` | `top_level` | `done` | `verified` | `tunnel-help.txt` | - | - | - |
| `pairing` | `top_level` | `done` | `verified` | `pairing-help.txt` | - | - | - |
| `acp shim` | `nested_surface` | `done` | `verified` | `acp-shim-help.txt` | - | `--session-id`, `--run-id`, `--prompt`, `--prompt-stdin`, `--ndjson-stdin` | - |
| `auth profiles` | `nested_surface` | `done` | `verified` | `auth-profiles-help.txt` | - | - | - |
| `auth access` | `nested_surface` | `done` | `verified` | `auth-access-help.txt` | - | - | - |
| `auth access backfill` | `nested_surface` | `done` | `verified` | `auth-access-backfill-help.txt` | - | `--dry-run`, `--json` | - |
| `auth openai` | `nested_surface` | `done` | `verified` | `auth-openai-help.txt` | - | - | - |
| `auth openai api-key` | `nested_surface` | `done` | `verified` | `auth-openai-api-key-help.txt` | - | `--profile-id`, `--api-key-stdin`, `--api-key-prompt`, `--set-default` | - |
| `auth profiles list` | `nested_surface` | `done` | `verified` | `auth-profiles-list-help.txt` | - | - | - |
| `auth profiles doctor` | `nested_surface` | `done` | `verified` | `auth-profiles-doctor-help.txt` | - | `--agent-id`, `--json` | - |
| `auth profiles audit` | `nested_surface` | `done` | `verified` | `auth-profiles-audit-help.txt` | - | `--agent-id`, `--provider`, `--provider-name`, `--json` | - |
| `auth profiles cooldown-clear` | `nested_surface` | `done` | `verified` | `auth-profiles-cooldown-clear-help.txt` | - | `--json` | - |
| `auth profiles order-set` | `nested_surface` | `done` | `verified` | `auth-profiles-order-set-help.txt` | - | `--provider`, `--provider-name`, `--agent-id`, `--json` | - |
| `auth profiles explain-selection` | `nested_surface` | `done` | `verified` | `auth-profiles-explain-selection-help.txt` | - | `--provider`, `--provider-name`, `--agent-id`, `--profile-id`, `--credential`, `--policy-denied-profile-id`, `--json` | - |
| `browser session` | `nested_surface` | `done` | `verified` | `browser-session-help.txt` | - | - | - |
| `browser session create` | `nested_surface` | `done` | `verified` | `browser-session-create-help.txt` | - | `--allow-private-targets`, `--allow-downloads`, `--allow-domain`, `--persistence-enabled` | - |
| `browser profiles` | `nested_surface` | `done` | `verified` | `browser-profiles-help.txt` | - | - | - |
| `browser profiles create` | `nested_surface` | `done` | `verified` | `browser-profiles-create-help.txt` | - | `--name`, `--persistence-enabled`, `--private-profile` | - |
| `browser tabs` | `nested_surface` | `done` | `verified` | `browser-tabs-help.txt` | - | - | - |
| `browser tabs open` | `nested_surface` | `done` | `verified` | `browser-tabs-open-help.txt` | - | `--url`, `--activate`, `--allow-private-targets` | - |
| `browser navigate` | `nested_surface` | `done` | `verified` | `browser-navigate-help.txt` | - | `--url`, `--timeout-ms`, `--allow-redirects`, `--allow-private-targets` | - |
| `browser upload` | `nested_surface` | `done` | `verified` | `browser-upload-help.txt` | - | `--selector`, `--file`, `--timeout-ms`, `--output`, `--json` | - |
| `browser downloads` | `nested_surface` | `done` | `verified` | `browser-downloads-help.txt` | - | `--artifact-id`, `--output`, `--max-bytes`, `--limit`, `--quarantined-only`, `--json` | - |
| `browser snapshot` | `nested_surface` | `done` | `verified` | `browser-snapshot-help.txt` | - | `--include-dom-snapshot`, `--include-visible-text`, `--output`, `--json` | - |
| `browser trace` | `nested_surface` | `done` | `verified` | `browser-trace-help.txt` | - | `--output` | - |
| `channels discord` | `nested_surface` | `done` | `verified` | `channels-discord-help.txt` | - | - | - |
| `channels discord setup` | `nested_surface` | `done` | `verified` | `channels-discord-setup-help.txt` | - | `--account-id`, `--verify-channel-id`, `--json` | - |
| `channels discord verify` | `nested_surface` | `done` | `verified` | `channels-discord-verify-help.txt` | `test-send` | `--account-id`, `--to`, `--text`, `--confirm` | - |
| `channels router` | `nested_surface` | `done` | `verified` | `channels-router-help.txt` | - | - | - |
| `channels router preview` | `nested_surface` | `done` | `verified` | `channels-router-preview-help.txt` | - | `--route-channel`, `--text`, `--requested-broadcast` | - |
| `config list` | `nested_surface` | `done` | `verified` | `config-list-help.txt` | `show` | `--path`, `--show-secrets` | - |
| `profile create` | `nested_surface` | `done` | `verified` | `profile-create-help.txt` | - | `--mode`, `--environment`, `--color`, `--risk-level`, `--strict-mode`, `--set-default`, `--force` | - |
| `profile delete` | `nested_surface` | `done` | `verified` | `profile-delete-help.txt` | - | `--yes`, `--delete-state-root` | - |
| `profile clone` | `nested_surface` | `done` | `verified` | `profile-clone-help.txt` | - | `--label`, `--environment`, `--color`, `--risk-level`, `--strict-mode`, `--set-default`, `--force` | - |
| `profile export` | `nested_surface` | `done` | `verified` | `profile-export-help.txt` | - | `--output`, `--mode`, `--password-stdin` | - |
| `profile import` | `nested_surface` | `done` | `verified` | `profile-import-help.txt` | - | `--input`, `--name`, `--password-stdin`, `--set-default`, `--force` | - |
| `cron update` | `nested_surface` | `done` | `verified` | `cron-update-help.txt` | `edit` | `--id`, `--prompt-stdin`, `--timezone`, `--max-runs` | - |
| `cron add` | `nested_surface` | `done` | `verified` | `cron-add-help.txt` | - | `--name`, `--prompt`, `--prompt-stdin`, `--schedule-type`, `--schedule`, `--timezone`, `--max-runs` | - |
| `cron delete` | `nested_surface` | `done` | `verified` | `cron-delete-help.txt` | `rm` | `--id` | - |
| `cron logs` | `nested_surface` | `done` | `verified` | `cron-logs-help.txt` | `runs` | `--id`, `--limit` | - |
| `routines upsert` | `nested_surface` | `done` | `verified` | `routines-upsert-help.txt` | `apply` | `--name`, `--prompt`, `--trigger-kind`, `--max-runs` | - |
| `routines create-from-template` | `nested_surface` | `done` | `verified` | `routines-create-from-template-help.txt` | - | `--template-id` | - |
| `routines import` | `nested_surface` | `done` | `verified` | `routines-import-help.txt` | - | - | - |
| `routines logs` | `nested_surface` | `done` | `verified` | `routines-logs-help.txt` | `runs` | `--id`, `--limit` | - |
| `routines delete` | `nested_surface` | `done` | `verified` | `routines-delete-help.txt` | `rm` | `--id` | - |
| `objectives upsert` | `nested_surface` | `done` | `verified` | `objectives-upsert-help.txt` | `apply` | `--kind`, `--name`, `--prompt` | - |
| `hooks bind` | `nested_surface` | `done` | `verified` | `hooks-bind-help.txt` | `install` | `--event`, `--plugin-id`, `--disabled` | - |
| `memory index` | `nested_surface` | `done` | `verified` | `memory-index-help.txt` | `reindex` | `--batch-size`, `--until-complete`, `--run-maintenance` | - |
| `plugins install` | `nested_surface` | `done` | `verified` | `plugins-install-help.txt` | `bind` | `--artifact`, `--config-json`, `--config-json-file`, `--config-json-stdin`, `--clear-config`, `--allow-tofu`, `--allow-untrusted`, `--json` | - |
| `plugins inspect` | `nested_surface` | `done` | `verified` | `plugins-inspect-help.txt` | `info` | `--json` | - |
| `plugins discover` | `nested_surface` | `done` | `verified` | `plugins-discover-help.txt` | - | `--plugin-id`, `--skill-id`, `--enabled-only`, `--ready-only`, `--json` | - |
| `plugins explain` | `nested_surface` | `done` | `verified` | `plugins-explain-help.txt` | - | `--json` | - |
| `plugins doctor` | `nested_surface` | `done` | `verified` | `plugins-doctor-help.txt` | - | `--plugin-id`, `--json` | - |
| `plugins update` | `nested_surface` | `done` | `verified` | `plugins-update-help.txt` | - | `--artifact`, `--config-json`, `--config-json-file`, `--config-json-stdin`, `--clear-config`, `--allow-tofu`, `--allow-untrusted`, `--json` | - |
| `sessions show` | `nested_surface` | `done` | `verified` | `sessions-show-help.txt` | `resume` | `--session-id`, `--session-key`, `--json` | - |
| `skills package` | `nested_surface` | `done` | `verified` | `skills-package-help.txt` | - | - | - |
| `skills package build` | `nested_surface` | `done` | `verified` | `skills-package-build-help.txt` | - | `--manifest`, `--sbom`, `--provenance`, `--output` | - |
| `support-bundle export` | `nested_surface` | `done` | `verified` | `support-bundle-export-help.txt` | - | `--output`, `--max-bytes`, `--journal-hash-limit`, `--error-limit` | - |
| `support-bundle replay-export` | `nested_surface` | `done` | `verified` | `support-bundle-replay-export-help.txt` | - | `--run-id`, `--output`, `--journal-db`, `--max-events` | - |
| `support-bundle replay-import` | `nested_surface` | `done` | `verified` | `support-bundle-replay-import-help.txt` | - | `--input`, `--output-dir` | - |
| `support-bundle replay-run` | `nested_surface` | `done` | `verified` | `support-bundle-replay-run-help.txt` | - | `--input`, `--diff-output` | - |
| `support-bundle replay-baseline` | `nested_surface` | `done` | `verified` | `support-bundle-replay-baseline-help.txt` | - | `--input`, `--output` | - |
| `system event` | `nested_surface` | `done` | `verified` | `system-event-help.txt` | `events` | - | - |
| `system insights` | `nested_surface` | `done` | `verified` | `system-insights-help.txt` | - | `--json` | - |
| `node install` | `nested_surface` | `done` | `verified` | `node-install-help.txt` | - | - | - |
| `nodes invoke` | `nested_surface` | `done` | `verified` | `nodes-invoke-help.txt` | - | - | - |
| `onboarding wizard` | `nested_surface` | `done` | `verified` | `onboarding-wizard-help.txt` | - | `--flow`, `--non-interactive`, `--accept-risk` | - |
| `webhooks test` | `nested_surface` | `done` | `verified` | `webhooks-test-help.txt` | - | - | - |
| `browser console` | `placeholder_surface` | `partial` | `verified` | `browser-console-help.txt` | - | `--output` | M42 kept a structured placeholder so the CLI tree stays stable before real console export lands. |
| `browser pdf` | `placeholder_surface` | `partial` | `verified` | `browser-pdf-help.txt` | - | `--output` | The command is discoverable and testable, but the implementation remains intentionally incomplete. |
| `browser select` | `placeholder_surface` | `partial` | `verified` | `browser-select-help.txt` | - | `--selector`, `--value` | - |
| `browser highlight` | `placeholder_surface` | `partial` | `verified` | `browser-highlight-help.txt` | - | `--selector` | - |
