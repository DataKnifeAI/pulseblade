use std::sync::Arc;

use pulseblade_core::Checkpoint;
use pulseblade_store::Store;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ServerCapabilities, ServerConfig},
    tool, tool_handler, tool_router, Json, ServerHandler,
};

use crate::query::{
    self, ChangeSet, ChangesParams, CheckpointParams, ExplainParams, Explanation, ExportParams,
    MetricsParams, Series, Snapshot, SnapshotParams,
};

const INSTRUCTIONS: &str = "\
Pulseblade is an infrastructure monitor built for agents. Typical loop:
1. state_snapshot: what exists and what is unhealthy right now (unhealthy sorts first).
2. Remember `as_of_seq`, or name the moment with checkpoint_create (e.g. before a deploy).
3. Later, changes_since with that checkpoint or `seq:<n>` to see only what changed.
4. resource_explain for one resource: attributes, children, latest metrics, and the recent changes \
on it and its parent (causal context).
5. metrics_query for bounded, downsampled history of one metric.
export_bulk returns JSONL when you need everything at once.
Resource ids: host:<host>, disk:<host>:<mount>, net:<host>:<iface>, unit:<host>:<unit>.";

/// MCP handler exposing Pulseblade's read tools.
#[derive(Clone)]
pub struct PulsebladeServer {
    store: Arc<Store>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl PulsebladeServer {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            tool_router: Self::tool_router(),
        }
    }

    async fn run<T, F>(&self, f: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce(&Store) -> query::Result<T> + Send + 'static,
    {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .map_err(|e| format!("internal error: {e}"))?
            .map_err(|e| e.to_string())
    }

    #[tool(
        description = "Current infrastructure state: resource counts by kind, then resources with health, labels, and latest metrics. Unhealthy resources sort first. Returns `as_of_seq` for use with changes_since.",
        annotations(title = "State snapshot", read_only_hint = true)
    )]
    async fn state_snapshot(
        &self,
        params: Parameters<SnapshotParams>,
    ) -> Result<Json<Snapshot>, String> {
        self.run(move |s| query::snapshot(s, params.0))
            .await
            .map(Json)
    }

    #[tool(
        description = "Everything about one resource: attributes, labels, parent, children summary (unhealthy children in full), latest metrics, and recent changes on it and its parent for causal context.",
        annotations(title = "Explain resource", read_only_hint = true)
    )]
    async fn resource_explain(
        &self,
        params: Parameters<ExplainParams>,
    ) -> Result<Json<Explanation>, String> {
        self.run(move |s| query::explain(s, params.0))
            .await
            .map(Json)
    }

    #[tool(
        description = "Downsampled time series for one metric of one resource, capped at 300 points, with min/max/avg/last stats.",
        annotations(title = "Query metrics", read_only_hint = true)
    )]
    async fn metrics_query(
        &self,
        params: Parameters<MetricsParams>,
    ) -> Result<Json<Series>, String> {
        self.run(move |s| query::metrics(s, params.0))
            .await
            .map(Json)
    }

    #[tool(
        description = "Name the current position in the change journal (e.g. `before-deploy`) so changes_since can later return only what changed after it. Reusing a name moves the checkpoint.",
        annotations(
            title = "Create checkpoint",
            read_only_hint = false,
            idempotent_hint = true,
            destructive_hint = false
        )
    )]
    async fn checkpoint_create(
        &self,
        params: Parameters<CheckpointParams>,
    ) -> Result<Json<Checkpoint>, String> {
        self.run(move |s| query::checkpoint(s, params.0))
            .await
            .map(Json)
    }

    #[tool(
        description = "Changes (appeared, changed with before/after, disappeared) since a checkpoint name, `seq:<n>`, RFC 3339 time, or relative duration like `15m`. Oldest first; follow `next_cursor` to page or to poll for future changes.",
        annotations(title = "Changes since", read_only_hint = true)
    )]
    async fn changes_since(
        &self,
        params: Parameters<ChangesParams>,
    ) -> Result<Json<ChangeSet>, String> {
        self.run(move |s| query::changes(s, params.0))
            .await
            .map(Json)
    }

    #[tool(
        description = "Bulk JSONL export: one line per resource (with latest metrics), then changes since `since` if given, then a meta line with `as_of_seq` and `truncated`.",
        annotations(title = "Bulk export", read_only_hint = true)
    )]
    async fn export_bulk(&self, params: Parameters<ExportParams>) -> Result<String, String> {
        self.run(move |s| query::export(s, params.0)).await
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for PulsebladeServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("pulseblade", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};
    use serde::Serialize;
    use serde_json::Value;

    fn seeded() -> Arc<Store> {
        let store = Store::open_in_memory().unwrap();
        let t = Utc::now() - Duration::minutes(1);
        store
            .apply(
                "host",
                t,
                &Observation {
                    resources: vec![Resource::new("host:h", ResourceKind::Host, "h")
                        .health(Health::Ok)
                        .attr("os", "linux")],
                    samples: vec![Sample::new("host:h", "cpu.used_pct", 5.0)],
                },
            )
            .unwrap();
        store
            .apply(
                "systemd",
                t,
                &Observation {
                    resources: vec![
                        Resource::new("unit:h:a.service", ResourceKind::Service, "a.service")
                            .parent("host:h")
                            .health(Health::Failed)
                            .attr("active", "failed"),
                        Resource::new("unit:h:b.service", ResourceKind::Service, "b.service")
                            .parent("host:h"),
                    ],
                    samples: vec![],
                },
            )
            .unwrap();
        store
            .apply(
                "systemd",
                Utc::now(),
                &Observation {
                    resources: vec![Resource::new(
                        "unit:h:a.service",
                        ResourceKind::Service,
                        "a.service",
                    )
                    .parent("host:h")
                    .health(Health::Ok)
                    .attr("active", "active")],
                    samples: vec![],
                },
            )
            .unwrap();
        Arc::new(store)
    }

    /// Clients such as Cursor reject structured content that violates the advertised schema.
    fn assert_matches_output_schema<T: Serialize>(server: &PulsebladeServer, tool: &str, out: T) {
        let tools = server.tool_router.list_all();
        let schema = tools
            .iter()
            .find(|t| t.name == tool)
            .and_then(|t| t.output_schema.clone())
            .unwrap_or_else(|| panic!("{tool} has no output schema"));
        let schema = Value::Object((*schema).clone());
        let validator = jsonschema::validator_for(&schema).unwrap();
        let value = serde_json::to_value(out).unwrap();
        let errors: Vec<String> = validator
            .iter_errors(&value)
            .map(|e| format!("{} at {}", e, e.instance_path()))
            .collect();
        assert!(errors.is_empty(), "{tool}: {errors:#?}\n{value:#}");
    }

    #[test]
    fn structured_outputs_match_advertised_schemas() {
        let store = seeded();
        let server = PulsebladeServer::new(store.clone());

        for verbose in [false, true] {
            let snap = query::snapshot(
                &store,
                SnapshotParams {
                    verbose: Some(verbose),
                    ..Default::default()
                },
            )
            .unwrap();
            assert_matches_output_schema(&server, "state_snapshot", snap);
        }
        for id in ["host:h", "unit:h:a.service", "unit:h:b.service"] {
            let e = query::explain(
                &store,
                ExplainParams {
                    id: id.into(),
                    changes_limit: None,
                },
            )
            .unwrap();
            assert_matches_output_schema(&server, "resource_explain", e);
        }
        for range in ["1h", "1s"] {
            let s = query::metrics(
                &store,
                MetricsParams {
                    id: "host:h".into(),
                    metric: "cpu.used_pct".into(),
                    range: Some(range.into()),
                    step: None,
                    agg: None,
                },
            )
            .unwrap();
            assert_matches_output_schema(&server, "metrics_query", s);
        }
        let cp = query::checkpoint(&store, CheckpointParams { name: "cp".into() }).unwrap();
        assert_matches_output_schema(&server, "checkpoint_create", cp);
        let cs = query::changes(
            &store,
            ChangesParams {
                since: "seq:0".into(),
                kind: None,
                resource_prefix: None,
                limit: None,
            },
        )
        .unwrap();
        assert!(cs.changes.iter().any(|c| c.field.is_some()));
        assert_matches_output_schema(&server, "changes_since", cs);
    }
}
