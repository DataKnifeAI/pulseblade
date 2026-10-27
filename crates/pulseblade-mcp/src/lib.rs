//! MCP server exposing Pulseblade state to agents, plus the node's read-only
//! HTTP surface (JSON API, Prometheus exposition, dashboard).

pub mod http;
pub mod journal;
pub mod prom;
pub mod query;
mod server;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use chrono::Utc;
use pulseblade_act::Registry;
use pulseblade_store::Store;

use crate::journal::Journal;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::ServiceExt;
use tokio_util::sync::CancellationToken;

pub use server::{ActionRoute, PulsebladeServer};

/// Serve MCP over stdin/stdout until the client disconnects.
pub async fn serve_stdio(store: Arc<Store>, actions: Arc<Registry>) -> anyhow::Result<()> {
    let service = PulsebladeServer::with_actions(store, actions)
        .serve(rmcp::transport::stdio())
        .await?;
    service.waiting().await?;
    Ok(())
}

/// Serve MCP at `/mcp`, the JSON API at `/api/v1`, Prometheus metrics at `/metrics`,
/// and the dashboard at `/` until `shutdown` fires. Requests whose `Host` is not in
/// `allowed_hosts` are rejected (an empty list allows all).
pub async fn serve_http(
    store: Arc<Store>,
    addr: SocketAddr,
    allowed_hosts: Vec<String>,
    shutdown: CancellationToken,
    actions: Arc<Registry>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_listener(listener, store, allowed_hosts, shutdown, actions).await
}

/// Serve on an already-bound listener so callers can discover port 0.
pub async fn serve_listener(
    listener: tokio::net::TcpListener,
    store: Arc<Store>,
    allowed_hosts: Vec<String>,
    shutdown: CancellationToken,
    actions: Arc<Registry>,
) -> anyhow::Result<()> {
    let router = build_router(
        Journal::always(store),
        allowed_hosts,
        shutdown.clone(),
        actions,
        None,
        Router::new(),
    );
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "listening: MCP http://{bound}/mcp, dashboard http://{bound}/");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

/// Dashboard, JSON API, `/mcp`, and optional extra routes. Every route sits behind
/// the host allow-list. `route` replaces local action execution with a hub router.
pub fn build_router(
    journal: Arc<Journal>,
    allowed_hosts: Vec<String>,
    shutdown: CancellationToken,
    actions: Arc<Registry>,
    route: Option<Arc<dyn ActionRoute>>,
    extra: Router,
) -> Router {
    let mut config = StreamableHttpServerConfig::default()
        .with_json_response(true)
        .with_cancellation_token(shutdown.child_token());
    if !allowed_hosts.is_empty() {
        config = config.with_allowed_hosts(allowed_hosts.clone());
    }
    let mcp_journal = journal.clone();
    let mcp_actions = actions.clone();
    let mcp_route = route.clone();
    let service = StreamableHttpService::new(
        move || {
            Ok(PulsebladeServer::with_journal(
                mcp_journal.clone(),
                mcp_actions.clone(),
                mcp_route.clone(),
            ))
        },
        Arc::new(LocalSessionManager::default()),
        config,
    );
    http::router_with_journal(journal, Utc::now(), actions)
        .merge(extra)
        .nest_service("/mcp", service)
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(allowed_hosts),
            http::check_host,
        ))
}
