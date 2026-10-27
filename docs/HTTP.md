# HTTP API

The node serves an LLM-free view of the same data on its HTTP port, next to `/mcp`. The approval webhook is the write. The page at `/` is documented in [DASHBOARD.md](DASHBOARD.md). Prometheus series, the Homepage widget, and remote access are in [DEPLOY.md](DEPLOY.md).

| Path | What |
|------|------|
| `/` | Built-in dashboard (one self-contained HTML page, no external assets) |
| `/metrics` | Prometheus text exposition |
| `/api/v1/status` | Pulseblade self-health: version, `as_of_seq`, database path and size, per-collector last run (age, duration, ok/error, consecutive failures), resource and sample counts, uptime. On a hub, `nodes` lists each node's cursor, `last_push_age_secs`, `stale`, and `shutting_down` after a shutdown push |
| `/api/v1/snapshot` | `state_snapshot`: `detail`, `if_changed_since`, `kind`, `query`, `labels=k=v,k2=v2`, `unhealthy_only`, `limit` |
| `/api/v1/resources/{id}` | `resource_explain` (`changes_limit`); ids may be percent-encoded or raw, e.g. `/api/v1/resources/disk:web1:/home` |
| `/api/v1/metrics` | `metrics_query`: `id`, `metric`, `range`, `step`, `agg` |
| `/api/v1/history` | `history`: `resource`, `metric`, `since` (duration or RFC 3339). Raw samples when they still cover `since`, otherwise 5-minute buckets |
| `/api/v1/search` | `search`: `text`, `since` (default `7d`), `resource`, `limit`, `cursor`. Failures, restarts, and error lines |
| `/api/v1/changes` | `changes_since`: `since` (default `1h`), `kind`, `prefix`, `limit`, `compact` |
| `/api/v1/checkpoints` | Checkpoints, newest first |
| `/api/v1/findings` | `findings_list`: `status`, `severity`, `limit`, `detail` |
| `/api/v1/logs` | `logs_query`: `resource`, `since`, `limit` |
| `/api/v1/actions` | Recent action runs |
| `POST /api/v1/actions/{id}/approve` | Approve a waiting run. Sends `Authorization: Bearer <actions.approve_token>` when that token is set |
| `GET /api/v1/portal` | Dashboard clock defaults (`timezone`, `time_format` as `12h` or `24h`) and the health cutoffs (`pod_restart_window_mins`, `pod_restart_lifetime`, `proc_cpu_share_failed`, `proc_mem_degraded`, `proc_mem_failed`). A browser may store its own zone and format |
| `PUT /api/v1/portal` | Save that health document. Unknown fields are rejected, including action tiers, allow-lists, `script.run`, and the clock defaults. A hub includes the saved document on `/hub/v1/poll` |
| `/healthz` | `ok` |

Errors are JSON `{"error": "..."}` with 400 for invalid input and 404 for unknown resources, metrics, checkpoints, or endpoints.
