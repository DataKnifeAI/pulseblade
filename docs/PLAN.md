# Plan

Pulseblade grows from a single host to a mixed fleet (hosts, containers, Proxmox, Kubernetes, cloud) without changing the agent-facing contract: MCP tools over a stable schema.

## Milestones

### M0 — Scaffold (done)

Cargo workspace, CI (`make vet`, `make test`), Makefile, license, README, architecture notes.

### M1 — Single-host agent (in progress)

- Host, CPU, memory, disk, network, and systemd service collectors.
- SQLite store: resource state, change journal, checkpoints, samples with retention.
- MCP tools over stdio and streamable HTTP: `state_snapshot`, `resource_explain`, `metrics_query`, `checkpoint_create`, `changes_since`, `export_bulk`.
- Semantic labels from config.
- Verified end-to-end with Cursor as the MCP client.

### M2 — Detection and memory

- `pulseblade-detect`: threshold rules (config) and EWMA/z-score anomaly detection.
- Findings carry causal context: recent changes on the resource, its parent, and its dependencies.
- Tools: `findings_list`, `finding_ack`, `memory_note`, `memory_search`.
- MCP resources `pulseblade://state` and `pulseblade://findings` with update notifications; optional outbound webhook.

### M3 — Gated remediation

- `pulseblade-act`: action registry in TOML. Each action has a JSON-schema for params, preconditions, an idempotency key, a risk tier (`auto` / `approve` / `deny`), dry-run, and rate limits.
- Tools: `actions_list`, `action_plan`, `action_execute`, `action_status`.
- Approval via `pulseblade ctl approve <run_id>` or webhook; hash-chained audit log.
- Built-ins: `systemd.restart`, `docker.restart`, `disk.prune_path` (allow-listed), `script.run` (allow-listed).
- `resource_explain` gains notes and prior actions.

### M4 — Docker and Proxmox

- Collectors behind `docker` (`bollard`) and `proxmox` (REST) features, with matching actions.

### M5 — Hub mode

- `pulseblade hub`: agents push deltas; hub holds fleet state and serves a global MCP endpoint.
- Action routing via agent long-poll (no inbound ports on agents); token or mTLS auth.
- Dockerfile and systemd unit.

### M6 — Kubernetes

- Collector and actions (rollout restart, scale) behind a `k8s` feature (`kube`).
- DaemonSet / Helm chart.
- Prometheus interop: scrape `/metrics` targets and expose `/metrics`.

### Later

- Cloud collectors (AWS, Azure, GCP).
- Minimal read-only web view.
