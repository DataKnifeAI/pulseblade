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
