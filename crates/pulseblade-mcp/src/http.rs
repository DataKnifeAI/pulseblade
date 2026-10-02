//! Read-only HTTP surface served beside `/mcp`: a JSON API under `/api/v1`,
//! Prometheus exposition at `/metrics`, and a built-in dashboard at `/`.
//!
//! Every handler goes through the same [`query`] layer as the MCP tools.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, uri::Authority, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use pulseblade_core::ResourceKind;
use pulseblade_store::{Store, StoreError};
use serde::Deserialize;
use serde_json::json;

use crate::prom;
use crate::query::{self, Detail, QueryError};

const DASHBOARD: &str = include_str!("dashboard.html");

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
    started: DateTime<Utc>,
}

/// Routes for `/`, `/metrics`, and `/api/v1/*`. `started` feeds `uptime_secs`.
pub fn router(store: Arc<Store>, started: DateTime<Utc>) -> Router {
    let api = Router::new()
        .route("/status", get(status))
        .route("/snapshot", get(snapshot))
        .route("/resources/{*id}", get(resource))
        .route("/metrics", get(metrics))
        .route("/changes", get(changes))
        .route("/checkpoints", get(checkpoints))
        .fallback(|| async { ApiError::not_found("no such API endpoint") });
    Router::new()
        .route("/", get(|| async { Html(DASHBOARD) }))
        .route("/metrics", get(prometheus))
        .nest("/api/v1", api)
        .with_state(AppState { store, started })
}

pub struct ApiError(StatusCode, String);

impl ApiError {
    fn not_found(msg: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, msg.into())
    }

    fn invalid(msg: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, msg.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<QueryError> for ApiError {
    fn from(e: QueryError) -> Self {
        let status = match &e {
            QueryError::NotFound(_) | QueryError::Store(StoreError::UnknownCheckpoint(_)) => {
                StatusCode::NOT_FOUND
            }
            QueryError::Invalid(_) => StatusCode::BAD_REQUEST,
            QueryError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self(status, e.to_string())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(e: QueryRejection) -> Self {
        Self::invalid(e.body_text())
    }
}

async fn blocking<T, F>(state: &AppState, f: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&Store) -> query::Result<T> + Send + 'static,
{
    let store = state.store.clone();
    tokio::task::spawn_blocking(move || f(&store))
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(Into::into)
}

type ApiResult = Result<Response, ApiError>;

fn ok<T: serde::Serialize>(v: T) -> ApiResult {
    Ok(Json(v).into_response())
}

async fn status(State(st): State<AppState>) -> ApiResult {
    let started = st.started;
    ok(blocking(&st, move |s| query::status(s, Some(started))).await?)
}

#[derive(Debug, Deserialize)]
struct SnapshotQuery {
    detail: Option<Detail>,
    if_changed_since: Option<i64>,
    kind: Option<ResourceKind>,
    query: Option<String>,
    /// `key=value` pairs separated by commas.
    labels: Option<String>,
    unhealthy_only: Option<bool>,
    limit: Option<usize>,
}

fn parse_labels(s: &str) -> Result<BTreeMap<String, String>, ApiError> {
    s.split(',')
        .filter(|p| !p.trim().is_empty())
        .map(|pair| {
            pair.split_once('=')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .ok_or_else(|| ApiError::invalid(format!("label `{pair}` is not key=value")))
        })
        .collect()
}

async fn snapshot(
    State(st): State<AppState>,
    q: Result<Query<SnapshotQuery>, QueryRejection>,
) -> ApiResult {
    let Query(q) = q?;
    let params = query::SnapshotParams {
        detail: q.detail,
        if_changed_since: q.if_changed_since,
        kind: q.kind,
        query: q.query,
        labels: q.labels.as_deref().map(parse_labels).transpose()?,
        unhealthy_only: q.unhealthy_only,
        limit: q.limit,
    };
    ok(blocking(&st, move |s| query::snapshot(s, params)).await?)
}

#[derive(Debug, Deserialize)]
struct ExplainQuery {
    changes_limit: Option<usize>,
}

async fn resource(
    State(st): State<AppState>,
    Path(id): Path<String>,
    q: Result<Query<ExplainQuery>, QueryRejection>,
) -> ApiResult {
    let Query(q) = q?;
    let params = query::ExplainParams {
        id,
        changes_limit: q.changes_limit,
    };
    ok(blocking(&st, move |s| query::explain(s, params)).await?)
}

async fn metrics(
    State(st): State<AppState>,
    q: Result<Query<query::MetricsParams>, QueryRejection>,
) -> ApiResult {
    let Query(params) = q?;
    ok(blocking(&st, move |s| query::metrics(s, params)).await?)
}

#[derive(Debug, Deserialize)]
struct ChangesQuery {
    /// Defaults to the last hour.
    since: Option<String>,
    kind: Option<ResourceKind>,
    prefix: Option<String>,
    limit: Option<usize>,
    compact: Option<bool>,
}

async fn changes(
    State(st): State<AppState>,
    q: Result<Query<ChangesQuery>, QueryRejection>,
) -> ApiResult {
    let Query(q) = q?;
    let params = query::ChangesParams {
        since: q.since.unwrap_or_else(|| "1h".into()),
        kind: q.kind,
        resource_prefix: q.prefix,
        limit: q.limit,
        compact: q.compact,
    };
    ok(blocking(&st, move |s| query::changes(s, params)).await?)
}

async fn checkpoints(State(st): State<AppState>) -> ApiResult {
    ok(blocking(&st, query::checkpoints).await?)
}

async fn prometheus(State(st): State<AppState>) -> ApiResult {
    let text = blocking(&st, prom::render).await?;
    Ok(([(header::CONTENT_TYPE, prom::CONTENT_TYPE)], text).into_response())
}

/// Reject requests whose `Host` is not in `allowed` (DNS-rebinding protection).
/// An empty list allows every host. Entries may carry a port (`example.com:8080`).
pub async fn check_host(
    State(allowed): State<Arc<Vec<String>>>,
    req: Request,
    next: Next,
) -> Response {
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string)
        .or_else(|| req.uri().authority().map(|a| a.to_string()));
    if allowed.is_empty() || host.is_some_and(|h| host_is_allowed(&h, &allowed)) {
        next.run(req).await
    } else {
        ApiError(
            StatusCode::FORBIDDEN,
            "Host header not allowed; add it to allowed_hosts in the config".into(),
        )
        .into_response()
    }
}

fn normalize_host(h: &str) -> String {
    h.trim_matches(['[', ']']).to_ascii_lowercase()
}

fn split_authority(s: &str) -> (String, Option<u16>) {
    match s.trim().parse::<Authority>() {
        Ok(a) => (normalize_host(a.host()), a.port_u16()),
        Err(_) => (normalize_host(s.trim()), None),
    }
}

fn host_is_allowed(host: &str, allowed: &[String]) -> bool {
    let (host, port) = split_authority(host);
    allowed.iter().any(|a| {
        let (ah, ap) = split_authority(a);
        ah == host && ap.is_none_or(|p| port == Some(p))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request as HttpRequest;
    use chrono::Duration;
    use pulseblade_core::{CollectorRun, Health, Observation, Resource, Sample};
    use serde_json::Value;
    use tower::ServiceExt;

    fn app() -> Router {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        for (i, cpu) in [(2, 10.0), (1, 20.0), (0, 30.0)] {
            store
                .apply(
                    "host",
                    now - Duration::minutes(i),
                    &Observation {
                        resources: vec![
                            Resource::new("host:h", ResourceKind::Host, "h")
                                .health(Health::Ok)
                                .label("host", "h"),
                            Resource::new("disk:h:/home", ResourceKind::Disk, "/home")
                                .parent("host:h")
                                .health(Health::Degraded)
                                .label("host", "h")
                                .attr("fs", "ext4"),
                        ],
                        samples: vec![
                            Sample::new("host:h", "cpu.used_pct", cpu),
                            Sample::new("disk:h:/home", "disk.used_pct", 93.0),
                        ],
                    },
                )
                .unwrap();
        }
        store
            .record_run(&CollectorRun {
                source: "host".into(),
                ts: now,
                duration_ms: 12,
                ok: true,
                error: None,
                resource_count: 2,
                sample_count: 2,
                change_count: 0,
                interval_secs: Some(15),
            })
            .unwrap();
        store.create_checkpoint("before", now).unwrap();
        router(Arc::new(store), now - Duration::seconds(90))
    }

    async fn get(app: &Router, uri: &str) -> (StatusCode, String, Option<String>) {
        let res = app
            .clone()
            .oneshot(HttpRequest::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let ctype = res
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_string());
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap(), ctype)
    }

    async fn get_json(app: &Router, uri: &str) -> (StatusCode, Value) {
        let (status, body, _) = get(app, uri).await;
        (status, serde_json::from_str(&body).unwrap())
    }

    #[tokio::test]
    async fn status_reports_self_health() {
        let app = app();
        let (code, v) = get_json(&app, "/api/v1/status").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(v["collectors"][0]["source"], "host");
        assert_eq!(v["collectors"][0]["ok"], true);
        assert_eq!(v["resources"]["total"], 2);
        assert_eq!(v["resources"]["degraded"], 1);
        assert!(v["uptime_secs"].as_i64().unwrap() >= 90);
        assert_eq!(v["samples"], 6);
    }

    #[tokio::test]
    async fn snapshot_modes_and_errors() {
        let app = app();
        let (code, v) = get_json(&app, "/api/v1/snapshot").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["detail"], "summary");
        assert_eq!(v["resources"][0]["id"], "disk:h:/home");

        let (_, v) = get_json(&app, "/api/v1/snapshot?kind=host").await;
        assert_eq!(v["detail"], "brief");
        assert_eq!(v["total"], 1);

        let (_, v) = get_json(&app, "/api/v1/snapshot?detail=full&labels=host=h").await;
        assert_eq!(v["total"], 2);
        assert_eq!(v["resources"][1]["labels"]["host"], "h");

        let seq = v["as_of_seq"].as_i64().unwrap();
        let (_, v) = get_json(&app, &format!("/api/v1/snapshot?if_changed_since={seq}")).await;
        assert_eq!(v, json!({"as_of_seq": seq, "unchanged": true}));

        let (code, v) = get_json(&app, "/api/v1/snapshot?kind=spaceship").await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(v["error"].is_string());
        let (code, _) = get_json(&app, "/api/v1/snapshot?labels=oops").await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn resources_metrics_changes_checkpoints() {
        let app = app();
        let (code, v) = get_json(&app, "/api/v1/resources/disk:h:/home").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["resource"]["attrs"]["fs"], "ext4");
        let (code, _) = get_json(&app, "/api/v1/resources/disk%3Ah%3A%2Fhome").await;
        assert_eq!(code, StatusCode::OK, "percent-encoded ids work too");
        let (code, v) = get_json(&app, "/api/v1/resources/unit:h:nope.service").await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        assert!(v["error"].as_str().unwrap().contains("no resource"));

        let (code, v) = get_json(
            &app,
            "/api/v1/metrics?id=host:h&metric=cpu.used_pct&range=10m&step=1m&agg=max",
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["values"], json!([10.0, 20.0, 30.0]));
        assert_eq!(v["step_secs"], 60);
        let (code, _) = get_json(&app, "/api/v1/metrics?id=host:h&metric=nope").await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        let (code, _) = get_json(
            &app,
            "/api/v1/metrics?id=host:h&metric=cpu.used_pct&range=x",
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        let (code, _) = get_json(&app, "/api/v1/metrics?metric=cpu.used_pct").await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "missing id");

        let (code, v) = get_json(&app, "/api/v1/changes?since=seq:0&compact=false").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["counts"]["appeared"], 2);
        assert!(v["changes"][0]["ts"].is_string());
        let (_, v) = get_json(&app, "/api/v1/changes?prefix=disk:").await;
        assert_eq!(v["total"], 1);
        assert!(v["changes"][0].get("ts").is_none());
        let (code, _) = get_json(&app, "/api/v1/changes?since=nope").await;
        assert_eq!(code, StatusCode::NOT_FOUND);

        let (code, v) = get_json(&app, "/api/v1/checkpoints").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v[0]["name"], "before");

        let (code, v) = get_json(&app, "/api/v1/nope").await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        assert!(v["error"].is_string());
    }

    #[tokio::test]
    async fn prometheus_and_dashboard() {
        let app = app();
        let (code, text, ctype) = get(&app, "/metrics").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(ctype.as_deref(), Some(prom::CONTENT_TYPE));
        assert!(text.contains("pulseblade_up 1"));
        assert!(text.contains("pulseblade_collector_up{collector=\"host\"} 1"));
        assert!(text.contains(
            "pulseblade_resource_health{id=\"disk:h:/home\",kind=\"disk\",host=\"h\"} 2"
        ));

        let (code, html, ctype) = get(&app, "/").await;
        assert_eq!(code, StatusCode::OK);
        assert!(ctype.unwrap().starts_with("text/html"));
        assert!(html.contains("/api/v1/status"));
    }

    #[test]
    fn host_matching() {
        let allowed: Vec<String> = ["localhost", "127.0.0.1", "::1", "mon.example.com:8443"]
            .map(String::from)
            .to_vec();
        assert!(host_is_allowed("localhost:7171", &allowed));
        assert!(host_is_allowed("LOCALHOST", &allowed));
        assert!(host_is_allowed("[::1]:7171", &allowed));
        assert!(host_is_allowed("mon.example.com:8443", &allowed));
        assert!(!host_is_allowed("mon.example.com", &allowed));
        assert!(!host_is_allowed("evil.example:7171", &allowed));
    }
}
