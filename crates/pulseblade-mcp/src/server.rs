use std::sync::Arc;

use pulseblade_core::Checkpoint;
use pulseblade_store::Store;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{
        Implementation, ListResourcesResult, ReadResourceRequestParams, ReadResourceResult,
        ResourceContents, ServerCapabilities, ServerConfig, SubscribeRequestParams,
        SubscriptionFilter,
    },
    service::{RequestContext, RoleServer, SubscriptionContext},
    tool, tool_handler, tool_router, ErrorData as McpError, Json, ServerHandler,
};
use serde::Serialize;

use crate::journal::{self, Journal, LiveOrStale, Read};
use crate::query::{
    self, AckParams, AckResult, ChangeSet, ChangesParams, CheckpointParams, EventPage,
    EventSearchParams, ExplainParams, Explanation, ExportParams, FindingPage, FindingsParams,
    FleetOverview, FleetParams, HistoryParams, HistorySeries, LogPage, LogsParams, MetricsParams,
    NotePage, NoteParams, SearchParams, Series, Snapshot, SnapshotParams,
};

/// Hub action calls. The node still plans and executes against its own policy.
pub trait ActionRoute: Send + Sync {
    fn plan(
        &self,
        params: pulseblade_act::PlanParams,
    ) -> Result<pulseblade_act::PlanResult, String>;
    fn execute(
        &self,
        params: pulseblade_act::ExecuteParams,
    ) -> Result<pulseblade_core::ActionRun, String>;
    fn status(
        &self,
        params: pulseblade_act::StatusParams,
    ) -> Result<pulseblade_core::ActionRun, String>;
}

const STATE_URI: &str = "pulseblade://state";
const FINDINGS_URI: &str = "pulseblade://findings";

fn known_uri(uri: &str) -> bool {
    uri == STATE_URI || uri == FINDINGS_URI
}

const INSTRUCTIONS: &str = "\
Pulseblade is an infrastructure monitor built for agents. Responses are small by default; \
ask for detail only where something changed. Cheap loop:
1. state_snapshot with no arguments: a summary (counts, collector health, host key metrics, \
unhealthy resources only). Remember `as_of_seq`, or name the moment with checkpoint_create.
2. To poll, call changes_since with `seq:<as_of_seq>` (or a checkpoint), or state_snapshot with \
`if_changed_since: <as_of_seq>`, which returns just `{as_of_seq, unchanged: true}` when nothing \
changed. Continue from `next_cursor`.
3. Drill down only on what changed or is unhealthy: resource_explain for one resource \
(attributes, children, latest metrics, recent changes, notes, and open findings), \
metrics_query or history for one metric (compact `values` array). history reads raw \
samples when they cover `since`, otherwise 5-minute buckets. search finds failures, \
restarts, and error lines (not info logs).
4. findings_list for open problems (detail=true adds the causal context and error lines). \
finding_ack to acknowledge one. logs_query for recent error lines. memory_note and \
memory_search for notes on a resource.
5. actions_list shows node-local remediation and its risk tier. action_plan does not run \
anything. action_execute runs `auto` immediately, leaves `approve` waiting for a human \
(`pulseblade ctl approve` or POST /api/v1/actions/{id}/approve), and rejects `deny`. \
action_status reads the run and says whether its finding is resolved, still open, \
or the approval is still waiting. Pass finding_id to action_plan to tie the run to one. \
Pass dry_run to plan when you only want the audit record. \
On a hub, those action calls are routed to the node that owns the resource; that node \
applies its own tier. fleet_overview groups the fleet by role, cluster, criticality, and \
host: unhealthy resources in full, everything else as counts.
Use state_snapshot filters (kind, query, labels) or detail=brief|full only when you need lists; \
export_bulk returns everything as JSONL and is large.
Collectors with `ok: false` or `stale: true` mean data is failing or going stale.
Resource ids: host:<host>, disk:<host>:<mount>, net:<host>:<iface>, unit:<host>:<unit>, \
unit:<host>:user:<unit>, proc:<host>:<pid>, session:<host>, ctr:<host>:<name>, \
img:<host>:<name>, node:<name>, vm:<node>:<vmid>, lxc:<node>:<vmid>, storage:<node>:<name>, \
knode:<cluster>:<name>, workload:<cluster>:<ns>:<kind>:<name>, pod:<cluster>:<ns>:<name>, \
prom:<host>:<job>.
MCP resources pulseblade://state and pulseblade://findings notify on change.";

/// MCP handler exposing Pulseblade's read tools.
#[derive(Clone)]
pub struct PulsebladeServer {
    journal: Arc<Journal>,
    actions: Arc<pulseblade_act::Registry>,
    route: Option<Arc<dyn ActionRoute>>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl PulsebladeServer {
    pub fn new(store: Arc<Store>) -> Self {
        Self::with_actions(store, Arc::new(pulseblade_act::Registry::builtin()))
    }

    pub fn with_actions(store: Arc<Store>, actions: Arc<pulseblade_act::Registry>) -> Self {
        Self::with_route(store, actions, None)
    }

    /// `route` sends action_plan, action_execute, and action_status to another tier.
    /// The local registry is still what `actions_list` describes.
    pub fn with_route(
        store: Arc<Store>,
        actions: Arc<pulseblade_act::Registry>,
        route: Option<Arc<dyn ActionRoute>>,
    ) -> Self {
        Self::with_journal(Journal::always(store), actions, route)
    }

    pub fn with_journal(
        journal: Arc<Journal>,
        actions: Arc<pulseblade_act::Registry>,
        route: Option<Arc<dyn ActionRoute>>,
    ) -> Self {
        Self {
            journal,
            actions,
            route,
            tool_router: Self::tool_router(),
        }
    }

    /// Writes fail closed. A database outage is not a successful ack or note.
    async fn run<T, F>(&self, f: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce(&Store) -> query::Result<T> + Send + 'static,
    {
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || match journal.store() {
            Some(store) => match f(&store) {
                Ok(value) => Ok(value),
                Err(query::QueryError::Store(err)) if journal::is_down(&err) => {
                    journal.mark_down();
                    Err("database unavailable".to_string())
                }
                Err(err) => Err(err.to_string()),
            },
            None => Err("database unavailable".to_string()),
        })
        .await
        .map_err(|e| format!("internal error: {e}"))?
    }

    /// Reads return the cached summary when the database is down.
    async fn read_out<T, F>(&self, f: F) -> Result<Json<LiveOrStale<T>>, String>
    where
        T: Serialize + schemars::JsonSchema + Send + 'static,
        F: FnOnce(&Store) -> query::Result<T> + Send + 'static,
    {
        let journal = self.journal.clone();
        let read = tokio::task::spawn_blocking(move || journal.read(f))
            .await
            .map_err(|e| format!("internal error: {e}"))?
            .map_err(|e| e.to_string())?;
        Ok(Json(match read {
            Read::Live(value) => LiveOrStale::Live(value),
            Read::Stale(snap) => LiveOrStale::Stale(snap),
        }))
    }

    #[tool(
        description = "Current infrastructure state. Default `summary`: counts by kind, collector health, host key metrics, and unhealthy resources with attributes. `brief` (default when filtering) lists every matching resource with health and latest metrics; `full` adds all labels and attributes. Pass `if_changed_since: <as_of_seq>` to get only `{as_of_seq, unchanged: true}` when nothing changed. Returns `as_of_seq` for changes_since.",
        annotations(title = "State snapshot", read_only_hint = true)
    )]
    async fn state_snapshot(
        &self,
        params: Parameters<SnapshotParams>,
    ) -> Result<Json<Snapshot>, String> {
        let journal = self.journal.clone();
        let read = tokio::task::spawn_blocking(move || {
            let read = journal.read(|s| query::snapshot(s, params.0))?;
            if let Read::Live(snap) = &read {
                journal.remember(snap);
            }
            Ok::<_, query::QueryError>(read)
        })
        .await
        .map_err(|e| format!("internal error: {e}"))?
        .map_err(|e| e.to_string())?;
        Ok(Json(match read {
            Read::Live(snap) => snap,
            Read::Stale(snap) => snap,
        }))
    }

    #[tool(
        description = "Everything about one resource: attributes, labels, parent, children summary (unhealthy children in full), latest metrics, recent changes on it and its parent, notes, open findings, and prior actions.",
        annotations(title = "Explain resource", read_only_hint = true)
    )]
    async fn resource_explain(
        &self,
        params: Parameters<ExplainParams>,
    ) -> Result<Json<LiveOrStale<Explanation>>, String> {
        self.read_out(move |s| query::explain(s, params.0)).await
    }

    #[tool(
        description = "Downsampled time series for one metric of one resource: `values[i]` covers the bucket at `start + i * step_secs` (null where empty), about 120 points by default and at most 300, with min/max/avg/last stats.",
        annotations(title = "Query metrics", read_only_hint = true)
    )]
    async fn metrics_query(
        &self,
        params: Parameters<MetricsParams>,
    ) -> Result<Json<LiveOrStale<Series>>, String> {
        self.read_out(move |s| query::metrics(s, params.0)).await
    }

    #[tool(
        description = "Downsampled history for one metric of one resource since a duration (`1h`, `7d`, `90d`) or an RFC 3339 timestamp. Raw samples when that window is still stored, otherwise 5-minute buckets. `values[i]` is the bucket at `start + i * step_secs` (null where empty), at most a few hundred points.",
        annotations(title = "Metric history", read_only_hint = true)
    )]
    async fn history(
        &self,
        params: Parameters<HistoryParams>,
    ) -> Result<Json<LiveOrStale<HistorySeries>>, String> {
        self.read_out(move |s| query::history(s, params.0)).await
    }

    #[tool(
        description = "Search failures, restarts, and error lines. Returns resource id, time, severity, and a short excerpt, newest first. Pass `cursor` from the previous page for older hits. Does not search info logs.",
        annotations(title = "Search events", read_only_hint = true)
    )]
    async fn search(
        &self,
        params: Parameters<EventSearchParams>,
    ) -> Result<Json<LiveOrStale<EventPage>>, String> {
        self.read_out(move |s| query::search(s, params.0)).await
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
        description = "Changes (appeared, changed with before/after, disappeared) since a checkpoint name, `seq:<n>`, RFC 3339 time, or relative duration like `15m`. Oldest first; follow `next_cursor` to page or to poll for future changes. Compact by default: `first_ts`/`last_ts` bound the set; pass `compact: false` for per-change `ts`.",
        annotations(title = "Changes since", read_only_hint = true)
    )]
    async fn changes_since(
        &self,
        params: Parameters<ChangesParams>,
    ) -> Result<Json<LiveOrStale<ChangeSet>>, String> {
        self.read_out(move |s| query::changes(s, params.0)).await
    }

    #[tool(
        description = "Bulk JSONL export: one line per resource (with latest metrics), then changes since `since` if given, then a meta line with `as_of_seq` and `truncated`.",
        annotations(title = "Bulk export", read_only_hint = true)
    )]
    async fn export_bulk(&self, params: Parameters<ExportParams>) -> Result<String, String> {
        let journal = self.journal.clone();
        let read =
            tokio::task::spawn_blocking(move || journal.read(|s| query::export(s, params.0)))
                .await
                .map_err(|e| format!("internal error: {e}"))?
                .map_err(|e| e.to_string())?;
        match read {
            Read::Live(text) => Ok(text),
            Read::Stale(snap) => {
                serde_json::to_string(&snap).map_err(|e| format!("internal error: {e}"))
            }
        }
    }

    #[tool(
        description = "Open findings by default (status, severity). Summaries only; pass detail=true for the causal context and recent error lines. Critical first, capped.",
        annotations(title = "List findings", read_only_hint = true)
    )]
    async fn findings_list(
        &self,
        params: Parameters<FindingsParams>,
    ) -> Result<Json<LiveOrStale<FindingPage>>, String> {
        self.read_out(move |s| query::findings(s, params.0)).await
    }

    #[tool(
        description = "Acknowledge an open finding. An optional note is stored on its resource and shows up in resource_explain.",
        annotations(
            title = "Acknowledge finding",
            read_only_hint = false,
            idempotent_hint = true,
            destructive_hint = false
        )
    )]
    async fn finding_ack(&self, params: Parameters<AckParams>) -> Result<Json<AckResult>, String> {
        self.run(move |s| query::ack_finding(s, params.0))
            .await
            .map(Json)
    }

    #[tool(
        description = "Attach a short note to a resource. Notes come back from resource_explain and memory_search.",
        annotations(
            title = "Remember a note",
            read_only_hint = false,
            idempotent_hint = false,
            destructive_hint = false
        )
    )]
    async fn memory_note(
        &self,
        params: Parameters<NoteParams>,
    ) -> Result<Json<pulseblade_core::Note>, String> {
        self.run(move |s| query::memory_note(s, params.0))
            .await
            .map(Json)
    }

    #[tool(
        description = "Search notes by a case-insensitive substring of the text or resource id. Newest first, capped.",
        annotations(title = "Search notes", read_only_hint = true)
    )]
    async fn memory_search(
        &self,
        params: Parameters<SearchParams>,
    ) -> Result<Json<LiveOrStale<NotePage>>, String> {
        self.read_out(move |s| query::memory_search(s, params.0))
            .await
    }

    #[tool(
        description = "Recent error log lines for one resource, newest first. Default window 1h, capped. Does not return a bulk journal.",
        annotations(title = "Query logs", read_only_hint = true)
    )]
    async fn logs_query(
        &self,
        params: Parameters<LogsParams>,
    ) -> Result<Json<LiveOrStale<LogPage>>, String> {
        self.read_out(move |s| query::logs(s, params.0)).await
    }

    #[tool(
        description = "Remediation actions this node allows, with risk tier (auto, approve, deny), parameter schema, preconditions, and hourly rate limit. Policy is node-local.",
        annotations(title = "List actions", read_only_hint = true)
    )]
    async fn actions_list(
        &self,
        _params: Parameters<pulseblade_act::ActionsParams>,
    ) -> Result<Json<pulseblade_act::ActionCatalog>, String> {
        Ok(Json(self.actions.list()))
    }

    #[tool(
        description = "Plan a remediation. Returns plan_id (also the run_id) and the risk tier. Does not run the command. Pass finding_id to tie the run to a finding. dry_run records a plan that execute will not invoke. Repeating an idempotency key returns the in-flight run.",
        annotations(
            title = "Plan action",
            read_only_hint = false,
            idempotent_hint = true,
            destructive_hint = false
        )
    )]
    async fn action_plan(
        &self,
        params: Parameters<pulseblade_act::PlanParams>,
    ) -> Result<Json<pulseblade_act::PlanResult>, String> {
        if !self.journal.is_up() {
            return Err("database unavailable".into());
        }
        if let Some(route) = self.route.clone() {
            return tokio::task::spawn_blocking(move || route.plan(params.0))
                .await
                .map_err(|e| format!("internal error: {e}"))?
                .map(Json);
        }
        let Some(store) = self.journal.store() else {
            return Err("database unavailable".into());
        };
        let journal = self.journal.clone();
        let actions = self.actions.clone();
        tokio::task::spawn_blocking(move || actions.plan(&store, params.0))
            .await
            .map_err(|e| format!("internal error: {e}"))?
            .map(Json)
            .map_err(|e| map_act(&journal, e))
    }

    #[tool(
        description = "Execute a planned action. auto runs now. approve returns awaiting_approval and does not run until a human approves. deny is rejected. A finished run is returned unchanged.",
        annotations(
            title = "Execute action",
            read_only_hint = false,
            idempotent_hint = true,
            destructive_hint = true
        )
    )]
    async fn action_execute(
        &self,
        params: Parameters<pulseblade_act::ExecuteParams>,
    ) -> Result<Json<pulseblade_core::ActionRun>, String> {
        if !self.journal.is_up() {
            return Err("database unavailable".into());
        }
        if let Some(route) = self.route.clone() {
            return tokio::task::spawn_blocking(move || route.execute(params.0))
                .await
                .map_err(|e| format!("internal error: {e}"))?
                .map(Json);
        }
        let Some(store) = self.journal.store() else {
            return Err("database unavailable".into());
        };
        let journal = self.journal.clone();
        let actions = self.actions.clone();
        let plan_id = params.0.plan_id;
        tokio::task::spawn_blocking(move || actions.execute(&store, &plan_id))
            .await
            .map_err(|e| format!("internal error: {e}"))?
            .map(Json)
            .map_err(|e| map_act(&journal, e))
    }

    #[tool(
        description = "Status of one action run, including output when it has finished. When the plan carried a finding_id, also says whether that finding is resolved, still open, or the approval is still waiting. Expired approvals are marked here.",
        annotations(title = "Action status", read_only_hint = true)
    )]
    async fn action_status(
        &self,
        params: Parameters<pulseblade_act::StatusParams>,
    ) -> Result<Json<LiveOrStale<pulseblade_core::ActionRun>>, String> {
        if !self.journal.is_up() {
            return Ok(Json(LiveOrStale::Stale(self.journal.stale())));
        }
        if let Some(route) = self.route.clone() {
            let run = tokio::task::spawn_blocking(move || route.status(params.0))
                .await
                .map_err(|e| format!("internal error: {e}"))??;
            return Ok(Json(LiveOrStale::Live(run)));
        }
        let Some(store) = self.journal.store() else {
            return Ok(Json(LiveOrStale::Stale(self.journal.stale())));
        };
        let journal = self.journal.clone();
        let actions = self.actions.clone();
        let run_id = params.0.run_id;
        match tokio::task::spawn_blocking(move || actions.status(&store, &run_id)).await {
            Ok(Ok(run)) => Ok(Json(LiveOrStale::Live(run))),
            Ok(Err(err)) => {
                if let pulseblade_act::ActError::Store(inner) = &err {
                    if journal::is_down(inner) {
                        journal.mark_down();
                        return Ok(Json(LiveOrStale::Stale(journal.stale())));
                    }
                }
                Err(err.to_string())
            }
            Err(err) => Err(format!("internal error: {err}")),
        }
    }

    #[tool(
        description = "Fleet rollup grouped by host and by the semantic labels role, cluster, and criticality. Unhealthy resources are listed in full; everything else is a count, so a large fleet stays small.",
        annotations(title = "Fleet overview", read_only_hint = true)
    )]
    async fn fleet_overview(
        &self,
        params: Parameters<FleetParams>,
    ) -> Result<Json<LiveOrStale<FleetOverview>>, String> {
        self.read_out(move |s| query::fleet_overview(s, params.0))
            .await
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for PulsebladeServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_resources_subscribe()
                .build(),
        )
        .with_server_info(Implementation::new("pulseblade", env!("CARGO_PKG_VERSION")))
        .with_instructions(INSTRUCTIONS)
    }

    async fn list_resources(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult::with_all_items(vec![
            rmcp::model::Resource::new(STATE_URI, "state")
                .with_title("Pulseblade state")
                .with_description(
                    "Summary snapshot: counts, collector health, unhealthy resources.",
                )
                .with_mime_type("application/json"),
            rmcp::model::Resource::new(FINDINGS_URI, "findings")
                .with_title("Pulseblade findings")
                .with_description(
                    "Open findings, without log lines. findings_list detail=true adds context.",
                )
                .with_mime_type("application/json"),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::ReadResourceResponse, McpError> {
        let uri = request.uri;
        if uri != STATE_URI && uri != FINDINGS_URI {
            return Err(McpError::resource_not_found(
                format!("unknown resource `{uri}`"),
                None,
            ));
        }
        let journal = self.journal.clone();
        let which = uri.clone();
        let read = tokio::task::spawn_blocking(move || {
            journal.read(|store| {
                if which == STATE_URI {
                    query::state_document(store)
                } else {
                    query::findings_document(store)
                }
            })
        })
        .await
        .map_err(|e| McpError::internal_error(e.to_string(), None))?
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let text = match read {
            Read::Live(text) => text,
            Read::Stale(snap) => serde_json::to_string(&snap)
                .map_err(|e| McpError::internal_error(e.to_string(), None))?,
        };
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, uri).with_mime_type("application/json")
        ])
        .into())
    }

    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        let mut filter = SubscriptionFilter::new();
        if let Some(uris) = &requested.resource_subscriptions {
            let accepted: Vec<String> = uris.iter().filter(|u| known_uri(u)).cloned().collect();
            if !accepted.is_empty() {
                filter.resource_subscriptions = Some(accepted);
            }
        }
        Some(filter)
    }

    async fn listen(&self, context: SubscriptionContext) -> Result<(), McpError> {
        let journal = self.journal.clone();
        let mut prev = read_revision(journal.clone()).await.unwrap_or((0, 0));
        loop {
            tokio::select! {
                () = context.cancelled() => return Ok(()),
                () = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
            }
            let Ok(next) = read_revision(journal.clone()).await else {
                continue;
            };
            let uris = context
                .accepted()
                .resource_subscriptions
                .clone()
                .unwrap_or_default();
            if next.0 != prev.0 && uris.iter().any(|u| u == STATE_URI) {
                let _ = context.sink().notify_resource_updated(STATE_URI).await;
            }
            if next.1 != prev.1 && uris.iter().any(|u| u == FINDINGS_URI) {
                let _ = context.sink().notify_resource_updated(FINDINGS_URI).await;
            }
            prev = next;
        }
    }

    fn subscribe(
        &self,
        request: SubscribeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<(), McpError>> + Send {
        if !known_uri(&request.uri) {
            return std::future::ready(Err(McpError::invalid_params(
                format!("unknown resource `{}`", request.uri),
                None,
            )));
        }
        let uri = request.uri;
        let journal = self.journal.clone();
        let peer = context.peer.clone();
        let ct = context.ct.clone();
        tokio::spawn(async move {
            let mut prev = read_revision(journal.clone()).await.unwrap_or((0, 0));
            loop {
                tokio::select! {
                    () = ct.cancelled() => break,
                    () = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                }
                let Ok(next) = read_revision(journal.clone()).await else {
                    continue;
                };
                let changed = (uri == STATE_URI && next.0 != prev.0)
                    || (uri == FINDINGS_URI && next.1 != prev.1);
                if changed {
                    let _ = peer
                        .notify_resource_updated(
                            rmcp::model::ResourceUpdatedNotificationParam::new(uri.clone()),
                        )
                        .await;
                }
                prev = next;
            }
        });
        std::future::ready(Ok(()))
    }
}

fn map_act(journal: &Journal, err: pulseblade_act::ActError) -> String {
    if let pulseblade_act::ActError::Store(inner) = &err {
        if journal::is_down(inner) {
            journal.mark_down();
            return "database unavailable".into();
        }
    }
    err.to_string()
}

async fn read_revision(journal: Arc<Journal>) -> Result<(i64, i64), String> {
    let Some(store) = journal.store() else {
        return Err("database unavailable".into());
    };
    let read = tokio::task::spawn_blocking(move || {
        store
            .current_seq()
            .and_then(|seq| Ok((seq, store.findings_token()?)))
    })
    .await
    .map_err(|e| e.to_string())?;
    match read {
        Ok(pair) => Ok(pair),
        Err(err) if journal::is_down(&err) => {
            journal.mark_down();
            Err("database unavailable".into())
        }
        Err(err) => Err(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use pulseblade_core::{CollectorRun, Health, Observation, Resource, ResourceKind, Sample};
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
                    logs: vec![],
                },
            )
            .unwrap();
        // Two older samples with a gap, so compact series contain nulls.
        for ago in [5, 3] {
            store
                .apply(
                    "host",
                    Utc::now() - Duration::minutes(ago),
                    &Observation {
                        resources: vec![Resource::new("host:h", ResourceKind::Host, "h")
                            .health(Health::Ok)
                            .attr("os", "linux")],
                        samples: vec![Sample::new("host:h", "cpu.used_pct", ago as f64)],
                        logs: vec![],
                    },
                )
                .unwrap();
        }
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
                    logs: vec![],
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
                    logs: vec![],
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
        store
            .record_run(&CollectorRun {
                source: "systemd".into(),
                ts: Utc::now(),
                duration_ms: 4,
                ok: false,
                error: Some("systemctl: boom".into()),
                resource_count: 0,
                sample_count: 0,
                change_count: 0,
                interval_secs: Some(15),
            })
            .unwrap();
        let seq = store.current_seq().unwrap();

        let detail_modes = [
            None,
            Some(query::Detail::Summary),
            Some(query::Detail::Brief),
            Some(query::Detail::Full),
        ];
        for detail in detail_modes {
            for if_changed_since in [None, Some(seq), Some(seq - 1)] {
                let snap = query::snapshot(
                    &store,
                    SnapshotParams {
                        detail,
                        if_changed_since,
                        ..Default::default()
                    },
                )
                .unwrap();
                assert_eq!(snap.unchanged, if_changed_since == Some(seq));
                assert_matches_output_schema(&server, "state_snapshot", snap);
            }
        }
        let filtered = query::snapshot(
            &store,
            SnapshotParams {
                query: Some(":h".into()),
                limit: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(filtered.truncated);
        assert_matches_output_schema(&server, "state_snapshot", filtered);
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
        for (range, step) in [("1h", None), ("1s", None), ("10m", Some("1m"))] {
            let s = query::metrics(
                &store,
                MetricsParams {
                    id: "host:h".into(),
                    metric: "cpu.used_pct".into(),
                    range: Some(range.into()),
                    step: step.map(Into::into),
                    agg: None,
                },
            )
            .unwrap();
            assert_matches_output_schema(&server, "metrics_query", s);
        }
        let gappy = query::metrics(
            &store,
            MetricsParams {
                id: "host:h".into(),
                metric: "cpu.used_pct".into(),
                range: Some("10m".into()),
                step: Some("1m".into()),
                agg: None,
            },
        )
        .unwrap();
        assert!(gappy.values.contains(&None), "{:?}", gappy.values);
        assert_matches_output_schema(&server, "metrics_query", gappy);

        let cp = query::checkpoint(&store, CheckpointParams { name: "cp".into() }).unwrap();
        assert_matches_output_schema(&server, "checkpoint_create", cp);
        for compact in [None, Some(false)] {
            let cs = query::changes(
                &store,
                ChangesParams {
                    since: "seq:0".into(),
                    kind: None,
                    resource_prefix: None,
                    limit: None,
                    compact,
                },
            )
            .unwrap();
            assert!(cs.changes.iter().any(|c| c.field.is_some()));
            assert_matches_output_schema(&server, "changes_since", cs);
        }
        let empty = query::changes(
            &store,
            ChangesParams {
                since: format!("seq:{seq}"),
                kind: None,
                resource_prefix: None,
                limit: None,
                compact: None,
            },
        )
        .unwrap();
        assert!(empty.changes.is_empty());
        assert_matches_output_schema(&server, "changes_since", empty);

        let opened = store
            .reconcile_findings(
                &[pulseblade_store::FindingDraft {
                    dedupe_key: "health\0unit:h:a.service".into(),
                    resource_id: "unit:h:a.service".into(),
                    rule: "health".into(),
                    severity: pulseblade_core::Severity::Critical,
                    summary: "a.service is failed".into(),
                    context: vec![pulseblade_core::Change {
                        seq: 1,
                        ts: Utc::now(),
                        resource_id: "unit:h:a.service".into(),
                        kind: pulseblade_core::ChangeKind::Changed,
                        field: Some("health".into()),
                        before: Some(serde_json::json!("ok")),
                        after: Some(serde_json::json!("failed")),
                    }],
                    logs: vec![pulseblade_core::LogLine {
                        ts: Utc::now(),
                        resource_id: "unit:h:a.service".into(),
                        message: "failed to start".into(),
                    }],
                }],
                Utc::now(),
            )
            .unwrap();
        let listed = query::findings(&store, query::FindingsParams::default()).unwrap();
        assert_eq!(listed.findings.len(), 1);
        assert!(listed.findings[0].context.is_empty());
        assert_matches_output_schema(&server, "findings_list", listed);
        let detailed = query::findings(
            &store,
            query::FindingsParams {
                detail: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!detailed.findings[0].logs.is_empty());
        assert_matches_output_schema(&server, "findings_list", detailed);
        let empty_findings = query::findings(
            &store,
            query::FindingsParams {
                status: Some(pulseblade_core::FindingStatus::Resolved),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(empty_findings.findings.is_empty());
        assert_matches_output_schema(&server, "findings_list", empty_findings);

        let acked = query::ack_finding(
            &store,
            query::AckParams {
                id: opened[0].id.clone(),
                note: Some("known during the deploy".into()),
            },
        )
        .unwrap();
        assert!(acked.note.is_some());
        assert_matches_output_schema(&server, "finding_ack", acked);
        let acked_again = query::ack_finding(
            &store,
            query::AckParams {
                id: opened[0].id.clone(),
                note: None,
            },
        )
        .unwrap();
        assert!(acked_again.note.is_none());
        assert_matches_output_schema(&server, "finding_ack", acked_again);

        let note = query::memory_note(
            &store,
            query::NoteParams {
                resource: "host:h".into(),
                text: "backup fills the disk".into(),
                author: None,
            },
        )
        .unwrap();
        assert_matches_output_schema(&server, "memory_note", note);
        let found = query::memory_search(
            &store,
            query::SearchParams {
                query: "backup".into(),
                limit: None,
            },
        )
        .unwrap();
        assert_eq!(found.notes.len(), 1);
        assert_matches_output_schema(&server, "memory_search", found);
        let none = query::memory_search(
            &store,
            query::SearchParams {
                query: "zzzz-no-such-note".into(),
                limit: Some(1),
            },
        )
        .unwrap();
        assert!(none.notes.is_empty());
        assert_matches_output_schema(&server, "memory_search", none);

        store
            .apply(
                "journald",
                Utc::now(),
                &Observation {
                    resources: vec![],
                    samples: vec![],
                    logs: vec![pulseblade_core::LogLine {
                        ts: Utc::now(),
                        resource_id: "unit:h:a.service".into(),
                        message: "accepted connection failed".into(),
                    }],
                },
            )
            .unwrap();
        let logs = query::logs(
            &store,
            query::LogsParams {
                resource: "unit:h:a.service".into(),
                since: Some("1h".into()),
                limit: Some(5),
            },
        )
        .unwrap();
        assert!(!logs.lines.is_empty());
        assert_matches_output_schema(&server, "logs_query", logs);
        let quiet = query::logs(
            &store,
            query::LogsParams {
                resource: "host:h".into(),
                since: None,
                limit: None,
            },
        )
        .unwrap();
        assert!(quiet.lines.is_empty());
        assert_matches_output_schema(&server, "logs_query", quiet);

        let hist = query::history(
            &store,
            query::HistoryParams {
                resource: "host:h".into(),
                metric: "cpu.used_pct".into(),
                since: "1h".into(),
            },
        )
        .unwrap();
        assert!(hist.stats.is_some());
        assert_matches_output_schema(&server, "history", &hist);
        let quiet_hist = query::history(
            &store,
            query::HistoryParams {
                resource: "host:h".into(),
                metric: "cpu.used_pct".into(),
                since: "1s".into(),
            },
        )
        .unwrap();
        assert!(quiet_hist.stats.is_none());
        assert!(quiet_hist.start.is_none());
        assert_matches_output_schema(&server, "history", &quiet_hist);

        let found = query::search(
            &store,
            query::EventSearchParams {
                text: "failed".into(),
                since: Some("7d".into()),
                resource: None,
                limit: Some(1),
                cursor: None,
            },
        )
        .unwrap();
        assert!(!found.hits.is_empty());
        assert_matches_output_schema(&server, "search", &found);
        let none = query::search(
            &store,
            query::EventSearchParams {
                text: "zzzz-no-such-event".into(),
                since: None,
                resource: Some("host:h".into()),
                limit: None,
                cursor: None,
            },
        )
        .unwrap();
        assert!(none.hits.is_empty());
        assert!(none.cursor.is_none());
        assert_matches_output_schema(&server, "search", &none);
        let bare = query::EventPage {
            as_of_seq: store.current_seq().unwrap(),
            hits: vec![query::EventHit {
                resource: "host:h".into(),
                ts: Utc::now(),
                severity: "warning".into(),
                excerpt: "health degraded".into(),
                kind: None,
            }],
            cursor: None,
        };
        assert_matches_output_schema(&server, "search", &bare);

        store
            .ingest_node(
                "h",
                &pulseblade_store::NodeBatch {
                    interval_secs: 15,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(store
            .set_node_last_push("h", (Utc::now() - Duration::seconds(90)).timestamp_millis(),)
            .unwrap());
        let stale_snap = query::snapshot(
            &store,
            SnapshotParams {
                detail: Some(query::Detail::Summary),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(stale_snap.nodes.iter().any(|n| n.node == "h" && n.stale));
        assert!(stale_snap.nodes.iter().all(|n| !n.shutting_down));
        assert_matches_output_schema(&server, "state_snapshot", &stale_snap);

        let explained = query::explain(
            &store,
            ExplainParams {
                id: "unit:h:a.service".into(),
                changes_limit: None,
            },
        )
        .unwrap();
        assert!(!explained.notes.is_empty());
        assert!(!explained.findings.is_empty());
        assert!(explained.actions.is_empty());
        assert_matches_output_schema(&server, "resource_explain", explained);

        let fleet = query::fleet_overview(&store, query::FleetParams::default()).unwrap();
        assert!(!fleet.unhealthy.is_empty() || fleet.hosts > 0 || fleet.as_of_seq >= 0);
        assert_matches_output_schema(&server, "fleet_overview", &fleet);
        let fleet_one = query::fleet_overview(
            &store,
            query::FleetParams {
                host: Some("h".into()),
            },
        )
        .unwrap();
        assert_matches_output_schema(&server, "fleet_overview", &fleet_one);

        struct Nop;
        impl pulseblade_act::CommandRunner for Nop {
            fn run(&self, _: &str, _: &[String]) -> Result<String, String> {
                Err("schema test must not run a command".into())
            }
        }
        let mut policy = pulseblade_act::ActionsConfig::default();
        policy.script_run.tier = Some(pulseblade_core::RiskTier::Auto);
        policy.script_run.allow = vec!["/usr/local/lib/pulseblade/hook".into()];
        let reg = pulseblade_act::Registry::new(policy, Arc::new(Nop)).unwrap();
        let catalog = reg.list();
        assert_eq!(catalog.actions.len(), 9);
        assert_matches_output_schema(&server, "actions_list", &catalog);
        let planned = reg
            .plan(
                &store,
                pulseblade_act::PlanParams {
                    action: "script.run".into(),
                    params: serde_json::json!({"script": "/usr/local/lib/pulseblade/hook"}),
                    resource_id: Some("host:h".into()),
                    idempotency_key: None,
                    dry_run: true,
                    finding_id: None,
                },
            )
            .unwrap();
        assert!(planned.dry_run);
        assert_matches_output_schema(&server, "action_plan", &planned);
        let executed = reg.execute(&store, &planned.plan_id).unwrap();
        assert_eq!(executed.status, pulseblade_core::ActionRunStatus::Succeeded);
        assert_matches_output_schema(&server, "action_execute", &executed);
        let status = reg.status(&store, &executed.id).unwrap();
        assert_matches_output_schema(&server, "action_status", &status);

        let waiting = reg
            .plan(
                &store,
                pulseblade_act::PlanParams {
                    action: "systemd.restart".into(),
                    params: serde_json::json!({"unit": "a.service"}),
                    resource_id: Some("unit:h:a.service".into()),
                    idempotency_key: Some("restart-a".into()),
                    dry_run: false,
                    finding_id: None,
                },
            )
            .unwrap();
        assert!(!waiting.dry_run);
        assert_matches_output_schema(&server, "action_plan", &waiting);
        let held = reg.execute(&store, &waiting.plan_id).unwrap();
        assert_eq!(
            held.status,
            pulseblade_core::ActionRunStatus::AwaitingApproval
        );
        assert_matches_output_schema(&server, "action_execute", &held);
        assert_matches_output_schema(
            &server,
            "action_status",
            reg.status(&store, &held.id).unwrap(),
        );

        let with_actions = query::explain(
            &store,
            ExplainParams {
                id: "host:h".into(),
                changes_limit: None,
            },
        )
        .unwrap();
        assert_eq!(with_actions.actions.len(), 1);
        assert_matches_output_schema(&server, "resource_explain", with_actions);

        let info = server.get_info();
        assert_eq!(
            info.capabilities
                .resources
                .as_ref()
                .and_then(|r| r.subscribe),
            Some(true)
        );
        assert_eq!(
            server.accepted_subscription_filter(
                &rmcp::model::SubscriptionFilter::builder()
                    .resource_subscription(STATE_URI)
                    .resource_subscription("pulseblade://nope")
                    .build()
            ),
            Some(
                rmcp::model::SubscriptionFilter::builder()
                    .resource_subscription(STATE_URI)
                    .build()
            )
        );
    }
}
