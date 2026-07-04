# Run Replay Diff

- Status: `failed`
- Events: `13`
- Diffs: `2`

| Path | Expected | Actual | Context |
| --- | --- | --- | --- |
| `$.expected.event_count` | `1` | `13` | trajectory event count |
| `$.expected.tool_proposals` | `["palyra.safe.read"]` | `["palyra.shell"]` | tool proposal stream changed |
