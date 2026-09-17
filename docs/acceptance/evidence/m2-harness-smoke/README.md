# M2 harness smoke evidence

Fake multi-agent run of the three fixed complex tasks in `docs/m2/tasks/` via
`python -m hibiki.interfaces.m2_runner --dry-run`.

| Field | Value |
| --- | --- |
| Result | 3/3 tasks produced EXECUTE runs and reached a terminal Fake path |
| Mode | dry-run (no live model) |

This is not the G6 live gate. Live evidence belongs under `m2-<date>/live-runs/`.
