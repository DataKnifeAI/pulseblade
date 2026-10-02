//! MCP server exposing Pulseblade state to agents.

pub mod query;
mod server;

use std::net::SocketAddr;
use std::sync::Arc;

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

/// Serve MCP over streamable HTTP at `http://<addr>/mcp` until `shutdown` fires.
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
        config = config.with_allowed_hosts(allowed_hosts);
    }
    let service = StreamableHttpService::new(
        move || Ok(PulsebladeServer::new(store.clone())),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .route("/healthz", axum::routing::get(|| async { "ok" }));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "MCP listening on http://{addr}/mcp");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}
