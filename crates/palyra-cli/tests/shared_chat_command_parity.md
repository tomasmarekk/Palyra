# Shared Chat Command Registry

This report is generated from the shared slash-command registry consumed by the web chat composer and the TUI.

## Summary

- Total commands: `30`
- Shared across web and TUI: `12`
- Web-visible commands: `14`
- TUI-visible commands: `28`
- Web-only commands: `2`
- TUI-only commands: `16`

## Entries

| Command | Synopsis | Category | Execution | Surfaces | Aliases | Capability tags | Entity targets |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `/help` | `/help` | `navigation` | `local` | `web`, `tui` | - | `help`, `palette` | - |
| `/status` | `/status` | `diagnostics` | `local` | `tui` | - | `status`, `diagnostics` | - |
| `/new` | `/new [label]` | `session` | `server` | `web`, `tui` | - | `session`, `create` | `session` |
| `/agent` | `/agent [agent-id]` | `agent` | `server` | `tui` | - | `agent`, `picker` | `agent` |
| `/session` | `/session [session-id-or-key]` | `session` | `server` | `tui` | - | `session`, `picker` | `session` |
| `/objective` | `/objective list|show|select|fire|pause|resume|archive|create` | `objective` | `server` | `tui` | - | `objective`, `mutating` | `objective` |
| `/heartbeat` | `/heartbeat list|show|select|fire|pause|resume|archive|create` | `objective` | `server` | `tui` | - | `objective`, `heartbeat` | `objective` |
| `/standing-order` | `/standing-order list|show|select|fire|pause|resume|archive|create` | `objective` | `server` | `tui` | - | `objective`, `standing_order` | `objective` |
| `/program` | `/program list|show|select|fire|pause|resume|archive|create` | `objective` | `server` | `tui` | - | `objective`, `program` | `objective` |
| `/history` | `/history [query]` | `session` | `local` | `web`, `tui` | - | `session`, `history`, `search` | `session` |
| `/resume` | `/resume [session-id-or-key]` | `session` | `server` | `web`, `tui` | - | `session`, `resume` | `session` |
| `/model` | `/model [model-id]` | `model` | `server` | `tui` | - | `model`, `picker` | `model` |
| `/reset` | `/reset` | `session` | `server` | `web`, `tui` | - | `session`, `reset` | `session` |
| `/retry` | `/retry` | `run` | `server` | `web`, `tui` | - | `run`, `retry` | `run` |
| `/branch` | `/branch [label]` | `session` | `server` | `web`, `tui` | - | `session`, `branch`, `lineage` | `session`, `run` |
| `/queue` | `/queue <text>` | `run` | `server` | `web`, `tui` | - | `run`, `queue` | `run` |
| `/delegate` | `/delegate <profile-or-template> <text>` | `run` | `server` | `web`, `tui` | - | `run`, `delegate`, `background` | `run`, `profile` |
| `/checkpoint` | `/checkpoint save|list|restore` | `session` | `server` | `tui` | - | `checkpoint`, `restore` | `session`, `checkpoint` |
| `/background` | `/background list|add|show|pause|resume|retry|cancel` | `background` | `server` | `tui` | - | `background`, `task`, `mutating` | `background_task` |
| `/abort` | `/abort [run-id]` | `run` | `server` | `tui` | - | `run`, `cancel` | `run` |
| `/usage` | `/usage` | `diagnostics` | `local` | `web`, `tui` | - | `usage`, `diagnostics` | - |
| `/compact` | `/compact [preview|apply|history]` | `session` | `server` | `web`, `tui` | - | `compact`, `session`, `history` | `session`, `compaction` |
| `/attach` | `/attach` | `attachment` | `local_capability` | `web`, `tui` | - | `attachment`, `local_capability` | `attachment` |
| `/search` | `/search <query>` | `search` | `server` | `web` | - | `search`, `transcript` | `session` |
| `/export` | `/export [json|markdown]` | `export` | `local_capability` | `web` | - | `export`, `download` | `session` |
| `/settings` | `/settings` | `local_ui` | `local` | `tui` | - | `settings`, `overlay` | - |
| `/tools` | `/tools on|off` | `local_ui` | `local` | `tui` | - | `tools`, `toggle` | - |
| `/thinking` | `/thinking on|off` | `local_ui` | `local` | `tui` | - | `thinking`, `toggle` | - |
| `/shell` | `/shell on|off` | `local_capability` | `local_capability` | `tui` | - | `shell`, `toggle`, `local_capability` | - |
| `/exit` | `/exit` | `navigation` | `local` | `tui` | `/quit` | `exit`, `quit` | - |
