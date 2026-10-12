# HTTP API

The node serves a read-only, LLM-free view of the same data on its HTTP port, next to `/mcp`. The page at `/` is documented in [DASHBOARD.md](DASHBOARD.md). Prometheus series, the Homepage widget, and remote access are in [DEPLOY.md](DEPLOY.md).

| Path | What |
|------|------|
| `/` | Built-in dashboard (one self-contained HTML page, no external assets) |
| `/metrics` | Prometheus text exposition |
| `/api/v1/status` | Pulseblade self-health: version, `as_of_seq`, database path and size, per-collector last run (age, duration, ok/error, consecutive failures), resource and sample counts, uptime. On a hub, `nodes` lists each node's cursor, `last_push_age_secs`, and `stale` |
| `/api/v1/snapshot` | `state_snapshot`: `detail`, `if_changed_since`, `kind`, `query`, `labels=k=v,k2=v2`, `unhealthy_only`, `limit` |
| `/api/v1/resources/{id}` | `resource_explain` (`changes_limit`); ids may be percent-encoded or raw, e.g. `/api/v1/resources/disk:web1:/home` |
| `/api/v1/metrics` | `metrics_query`: `id`, `metric`, `range`, `step`, `agg` |
| `/api/v1/changes` | `changes_since`: `since` (default `1h`), `kind`, `prefix`, `limit`, `compact` |
| `/api/v1/checkpoints` | Checkpoints, newest first |
| `/api/v1/findings` | `findings_list`: `status`, `severity`, `limit`, `detail` |
| `/api/v1/logs` | `logs_query`: `resource`, `since`, `limit` |
| `/api/v1/actions` | Recent action runs |
| `POST /api/v1/actions/{id}/approve` | Approve a waiting run. Sends `Authorization: Bearer <actions.approve_token>` when that token is set |
| `/healthz` | `ok` |

Errors are JSON `{"error": "..."}` with 400 for invalid input and 404 for unknown resources, metrics, checkpoints, or endpoints.
