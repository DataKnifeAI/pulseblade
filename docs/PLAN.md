# Plan

Pulseblade grows from a single box to a mixed fleet (hosts, containers, Proxmox, Kubernetes, cloud) without changing the agent-facing contract: MCP tools over a stable schema. The single-box path (node + pilot) comes first; the fleet path (hub) builds on it. Roles and invariants are in [ARCHITECTURE.md](ARCHITECTURE.md).

## Milestones

### M0 — Scaffold (done)

Cargo workspace, CI (`make vet`, `make test`), Makefile, license, README, architecture notes.

### M1 — Single-host node (done)

- Host, CPU, memory, disk, network, and systemd service collectors.
- SQLite store: resource state, change journal, checkpoints, samples with retention.
- MCP tools over stdio and streamable HTTP: `state_snapshot`, `resource_explain`, `metrics_query`, `checkpoint_create`, `changes_since`, `export_bulk`.
- Semantic labels from config.
- Verified end-to-end with Cursor as the MCP client.

### M1.1 — Dashboard and HTTP API (done)

- LLM-free basic view on the node's HTTP port: JSON API under `/api/v1` (status, snapshot, resources, metrics, changes, checkpoints) built on the shared query layer, Prometheus exposition at `/metrics`, and an embedded single-page dashboard at `/`.
- Self-observability: collector runs persisted in the store; collector health in `state_snapshot`, `/api/v1/status`, Prometheus, and `pulseblade ctl status` (plain-text summary).
- Token-efficient MCP: `state_snapshot` `detail` (`summary` default, `brief`, `full`) and `if_changed_since`, compact `metrics_query` series, compact `changes_since`, measured in a token-budget test (see [ARCHITECTURE.md](ARCHITECTURE.md#token-efficiency)).
- Docs for reverse-proxy access, Prometheus/Grafana, and Homepage.

### M2 — OS and user signals, detection, memory ([#1](https://github.com/DataKnifeAI/pulseblade/issues/1))

- Collectors: journald (per-unit error rate, recent error lines), top processes by CPU and memory, user systemd units (`systemctl --user`) and session.
- `pulseblade-detect`: threshold rules (config) and EWMA/z-score anomaly detection.
- Findings carry causal context: recent changes and log lines on the resource, its parent, and its dependencies.
- Tools: `findings_list`, `finding_ack`, `memory_note`, `memory_search`, `logs_query`.
- MCP resources `pulseblade://state` and `pulseblade://findings` with update notifications; optional outbound webhook.

### M3 — Gated remediation ([#2](https://github.com/DataKnifeAI/pulseblade/issues/2))

- `pulseblade-act`: action registry in TOML. Each action has a JSON-schema for params, preconditions, an idempotency key, a risk tier (`auto` / `approve` / `deny`), dry-run, and rate limits. Policy is node-local.
- Tools: `actions_list`, `action_plan`, `action_execute`, `action_status`.
- Approval via `pulseblade ctl approve <run_id>` or webhook; hash-chained audit log.
- Built-ins: `systemd.restart`, `docker.restart`, `disk.prune_path` (allow-listed), `script.run` (allow-listed).
- `resource_explain` gains notes and prior actions.

### M4 — Pilot ([#6](https://github.com/DataKnifeAI/pulseblade/issues/6))

- `pulseblade-pilot` crate and `pulseblade pilot` command: an MCP client driven by any OpenAI-compatible endpoint, local (e.g. Ollama, vLLM) or remote (e.g. OpenRouter), with tiered routing and fallback ([design](ARCHITECTURE.md#pilot-planned-m4)).
- Event-driven: wakes on new findings and a slow heartbeat, never per collection pass.
- Investigates with the read tools, writes conclusions as notes, proposes actions through `action_plan`; `auto`-tier actions only if the operator enables it.
- Budgets: max tool calls, tokens, and wall time per investigation; every run recorded.
- Works against a node or a hub without changes.

### M5 — Docker and Proxmox ([#3](https://github.com/DataKnifeAI/pulseblade/issues/3))

- Collectors behind `docker` (`bollard`) and `proxmox` (REST) features, with matching actions.

### M6 — Hub ([#4](https://github.com/DataKnifeAI/pulseblade/issues/4))

- `pulseblade hub`: per-node journal cursors, catch-up after disconnect, fleet state, and the same MCP tools fleet-wide.
- `fleet_overview`: grouped by labels and host, unhealthy in full, the rest as counts.
- Action routing via node long-poll (no inbound ports on nodes); nodes enforce their own policy.
- Token or mTLS auth; Dockerfile and systemd unit.

### M7 — Kubernetes ([#5](https://github.com/DataKnifeAI/pulseblade/issues/5))

- Collector and actions (rollout restart, scale) behind a `k8s` feature (`kube`).
- DaemonSet / Helm chart.
- Prometheus interop: scrape `/metrics` targets (exposing `/metrics` shipped in M1.1).

### Later

- Cloud collectors (AWS, Azure, GCP).
- Hub-of-hubs (a hub presenting itself as a node upstream).
