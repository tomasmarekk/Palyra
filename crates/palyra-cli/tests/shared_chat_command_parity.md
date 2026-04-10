# Shared Chat Command Registry

This report is generated from the shared slash-command registry consumed by the web chat composer and the TUI.

## Summary

- Total commands: `34`
- Shared across web and TUI: `19`
- Web-visible commands: `21`
- TUI-visible commands: `32`
- Web-only commands: `2`
- TUI-only commands: `13`

## Entries

| Command | Synopsis | Category | Execution | Surfaces | Aliases | Capability tags | Entity targets |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `/help` | `/help` | `navigation` | `local` | `web`, `tui` | - | `help`, `palette` | - |
| `/status` | `/status` | `diagnostics` | `local` | `tui` | - | `status`, `diagnostics` | - |
| `/new` | `/new [label]` | `session` | `server` | `web`, `tui` | - | `session`, `create` | `session` |
| `/agent` | `/agent [agent-id]` | `agent` | `server` | `tui` | - | `agent`, `picker` | `agent` |
| `/session` | `/session [session-id-or-key]` | `session` | `server` | `tui` | - | `session`, `picker` | `session` |
| `/objective` | `/objective [objective-id-or-name]` | `objective` | `server` | `web`, `tui` | - | `objective`, `navigation` | `objective` |
| `/heartbeat` | `/heartbeat list|show|select|fire|pause|resume|archive|create` | `objective` | `server` | `tui` | - | `objective`, `heartbeat` | `objective` |
| `/standing-order` | `/standing-order list|show|select|fire|pause|resume|archive|create` | `objective` | `server` | `tui` | - | `objective`, `standing_order` | `objective` |
| `/program` | `/program list|show|select|fire|pause|resume|archive|create` | `objective` | `server` | `tui` | - | `objective`, `program` | `objective` |
| `/history` | `/history [query]` | `session` | `local` | `web`, `tui` | - | `session`, `history`, `search` | `session` |
| `/resume` | `/resume [session-id-or-key]` | `session` | `server` | `web`, `tui` | - | `session`, `resume` | `session` |
| `/model` | `/model [model-id]` | `model` | `server` | `tui` | - | `model`, `picker` | `model` |
| `/undo` | `/undo [checkpoint-id]` | `session` | `server` | `web`, `tui` | - | `undo`, `checkpoint`, `restore` | `checkpoint`, `session`, `run` |
| `/interrupt` | `/interrupt [soft|force] [redirect-prompt]` | `run` | `server` | `web`, `tui` | `/abort`, `/cancel` | `run`, `interrupt`, `cancel`, `redirect` | `run` |
| `/reset` | `/reset` | `session` | `server` | `web`, `tui` | - | `session`, `reset` | `session` |
| `/retry` | `/retry` | `run` | `server` | `web`, `tui` | - | `run`, `retry` | `run` |
| `/branch` | `/branch [label]` | `session` | `server` | `web`, `tui` | - | `session`, `branch`, `lineage` | `session`, `run` |
| `/queue` | `/queue <text>` | `run` | `server` | `web`, `tui` | - | `run`, `queue` | `run` |
| `/delegate` | `/delegate <profile-or-template> <text>` | `run` | `server` | `web`, `tui` | - | `run`, `delegate`, `background` | `run`, `profile` |
| `/checkpoint` | `/checkpoint list|restore <checkpoint-id>|save <name>` | `session` | `server` | `web`, `tui` | - | `checkpoint`, `restore` | `session`, `checkpoint` |
| `/background` | `/background list|add|show|pause|resume|retry|cancel` | `background` | `server` | `tui` | - | `background`, `task`, `mutating` | `background_task` |
| `/usage` | `/usage` | `diagnostics` | `local` | `web`, `tui` | - | `usage`, `diagnostics` | - |
| `/compact` | `/compact [preview|apply|history]` | `session` | `server` | `web`, `tui` | - | `compact`, `session`, `history` | `session`, `compaction` |
| `/attach` | `/attach` | `attachment` | `local_capability` | `web`, `tui` | - | `attachment`, `local_capability` | `attachment` |
| `/profile` | `/profile [profile-id-or-name]` | `profile` | `local` | `web`, `tui` | - | `profile`, `auth`, `navigation` | `profile` |
| `/browser` | `/browser [profile-id-or-session-id]` | `browser` | `local` | `web`, `tui` | - | `browser`, `navigation`, `profile` | `browser_profile`, `browser_session` |
| `/doctor` | `/doctor [jobs|run|repair]` | `diagnostics` | `server` | `web`, `tui` | - | `doctor`, `diagnostics`, `recovery` | `doctor_job` |
| `/search` | `/search <query>` | `search` | `server` | `web` | - | `search`, `transcript` | `session` |
| `/export` | `/export [json|markdown]` | `export` | `local_capability` | `web` | - | `export`, `download` | `session` |
| `/settings` | `/settings` | `local_ui` | `local` | `tui` | - | `settings`, `overlay` | - |
| `/tools` | `/tools on|off` | `local_ui` | `local` | `tui` | - | `tools`, `toggle` | - |
| `/thinking` | `/thinking on|off` | `local_ui` | `local` | `tui` | - | `thinking`, `toggle` | - |
| `/shell` | `/shell on|off` | `local_capability` | `local_capability` | `tui` | - | `shell`, `toggle`, `local_capability` | - |
| `/exit` | `/exit` | `navigation` | `local` | `tui` | `/quit` | `exit`, `quit` | - |
