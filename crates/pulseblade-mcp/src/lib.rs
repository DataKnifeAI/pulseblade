//! MCP server exposing Pulseblade state to agents, plus the node's read-only
//! HTTP surface (JSON API, Prometheus exposition, dashboard).

pub mod http;
pub mod prom;
pub mod query;
mod server;

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use pulseblade_store::Store;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::ServiceExt;
use tokio_util::sync::CancellationToken;

pub use server::PulsebladeServer;

/// Serve MCP over stdin/stdout until the client disconnects.
pub async fn serve_stdio(store: Arc<Store>) -> anyhow::Result<()> {
    let service = PulsebladeServer::new(store)
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
) -> anyhow::Result<()> {
    let mut config = StreamableHttpServerConfig::default()
        .with_json_response(true)
        .with_cancellation_token(shutdown.child_token());
    if !allowed_hosts.is_empty() {
        config = config.with_allowed_hosts(allowed_hosts.clone());
    }
    let mcp_store = store.clone();
    let service = StreamableHttpService::new(
        move || Ok(PulsebladeServer::new(mcp_store.clone())),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let router = http::router(store, Utc::now())
        .nest_service("/mcp", service)
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(allowed_hosts),
            http::check_host,
        ));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening: MCP http://{addr}/mcp, dashboard http://{addr}/");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}
