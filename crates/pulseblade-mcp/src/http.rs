//! Read-only HTTP surface served beside `/mcp`: a JSON API under `/api/v1`,
//! Prometheus exposition at `/metrics`, and a built-in dashboard at `/`.
//!
//! Every handler goes through the same [`query`] layer as the MCP tools.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Path, Query, Request, State};
use axum::http::HeaderMap;
use axum::http::{header, uri::Authority, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use pulseblade_core::{
    default_time_format, default_timezone, valid_time_format, valid_timezone, HealthThresholds,
    ResourceKind,
};
use pulseblade_store::{Store, StoreError};
use serde::Deserialize;
use serde_json::json;

use crate::journal::{self, Journal, Read};
use crate::prom;
use crate::query::{self, Detail, QueryError};
use crate::server::ActionRoute;

const DASHBOARD: &str = include_str!("dashboard.html");
const FAVICON_PNG: &[u8] = include_bytes!("../../../docs/assets/dashboard/favicon-32.png");

/// Compiled or toml defaults the dashboard shows until a portal save is stored.
#[derive(Clone, Debug)]
pub struct PortalDefaults {
    pub timezone: String,
    pub time_format: String,
    pub health: HealthThresholds,
}

impl Default for PortalDefaults {
    fn default() -> Self {
        Self {
            timezone: default_timezone(),
            time_format: default_time_format(),
            health: HealthThresholds::default(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    journal: Arc<Journal>,
    started: DateTime<Utc>,
    actions: Arc<pulseblade_act::Registry>,
    portal: PortalDefaults,
    /// Set on a hub. Approve of a routed run is sent to the node that holds it.
    route: Option<Arc<dyn ActionRoute>>,
}

/// Routes for `/`, `/metrics`, and `/api/v1/*`. `started` feeds `uptime_secs`.
pub fn router(store: Arc<Store>, started: DateTime<Utc>) -> Router {
    router_with(
        store,
        started,
        Arc::new(pulseblade_act::Registry::builtin()),
    )
}

/// Same routes with the node's action registry (approval and the action list).
pub fn router_with(
    store: Arc<Store>,
    started: DateTime<Utc>,
    actions: Arc<pulseblade_act::Registry>,
) -> Router {
    router_with_journal(
        journal::Journal::always(store),
        started,
        actions,
        PortalDefaults::default(),
        None,
    )
}

/// Same routes, using a hub journal that can serve a stale snapshot.
pub fn router_with_journal(
    journal: Arc<Journal>,
    started: DateTime<Utc>,
    actions: Arc<pulseblade_act::Registry>,
    portal: PortalDefaults,
    route: Option<Arc<dyn ActionRoute>>,
) -> Router {
    let api = Router::new()
        .route("/status", get(status))
        .route("/snapshot", get(snapshot))
        .route("/resources/{*id}", get(resource))
        .route("/metrics", get(metrics))
        .route("/history", get(history))
        .route("/search", get(search_events))
        .route("/changes", get(changes))
        .route("/checkpoints", get(checkpoints))
        .route("/findings", get(findings))
        .route("/logs", get(logs))
        .route("/actions", get(actions_list))
        .route("/actions/{id}/approve", post(approve_action))
        .route("/actions/{id}/deny", post(deny_action))
        .route("/actions/{id}/dismiss", post(dismiss_action))
        .route("/actions/{id}/recheck", post(recheck_action))
        .route("/portal", get(portal_get).put(portal_put))
        .fallback(|| async { ApiError::not_found("no such API endpoint") });
    Router::new()
        .route("/", get(dashboard))
        .route("/favicon.ico", get(favicon))
        .route("/metrics", get(prometheus))
        .nest("/api/v1", api)
        .with_state(AppState {
            journal,
            started,
            actions,
            portal,
            route,
        })
}

fn dashboard_timezone(portal: &PortalDefaults) -> String {
    if valid_timezone(&portal.timezone) {
        portal.timezone.clone()
    } else {
        default_timezone()
    }
}

fn dashboard_time_format(portal: &PortalDefaults) -> String {
    if valid_time_format(&portal.time_format) {
        portal.time_format.clone()
    } else {
        default_time_format()
    }
}

async fn dashboard(State(st): State<AppState>) -> Html<String> {
    let html = DASHBOARD
        .replace("__PB_TIMEZONE__", &dashboard_timezone(&st.portal))
        .replace("__PB_TIME_FORMAT__", &dashboard_time_format(&st.portal));
    Html(html)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortalWrite {
    health: HealthThresholds,
}

async fn portal_get(State(st): State<AppState>) -> ApiResult {
    let defaults = st.portal.health.clone();
    let journal = st.journal.clone();
    let health = tokio::task::spawn_blocking(move || {
        let Some(store) = journal.store() else {
            return defaults;
        };
        match store.health_thresholds() {
            Ok(Some(doc)) => doc,
            _ => defaults,
        }
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    ok(json!({
        "timezone": dashboard_timezone(&st.portal),
        "time_format": dashboard_time_format(&st.portal),
        "health": health,
    }))
}

async fn portal_put(
    State(st): State<AppState>,
    body: Result<Json<PortalWrite>, axum::extract::rejection::JsonRejection>,
) -> ApiResult {
    let Json(body) = body.map_err(|e| ApiError::invalid(e.body_text()))?;
    body.health.validate().map_err(ApiError::invalid)?;
    let journal = st.journal.clone();
    let health = body.health;
    tokio::task::spawn_blocking(move || {
        let Some(store) = journal.store() else {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "database unavailable".into(),
            ));
        };
        match store.set_health_thresholds(&health) {
            Ok(()) => Ok(health),
            Err(err) if journal::is_down(&err) => {
                journal.mark_down();
                Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "database unavailable".into(),
                ))
            }
            Err(StoreError::Invalid(msg)) => Err(ApiError::invalid(msg)),
            Err(err) => Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
        }
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;
    portal_get(State(st)).await
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

async fn read_on<T, F>(journal: Arc<Journal>, f: F) -> Result<Read<T>, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&Store) -> query::Result<T> + Send + 'static,
{
    match tokio::task::spawn_blocking(move || journal.read(f)).await {
        Ok(Ok(read)) => Ok(read),
        Ok(Err(err)) => Err(err.into()),
        Err(err) => Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }
}

/// Live JSON, or the cached summary when the database is down.
async fn blocking<T, F>(state: &AppState, f: F) -> ApiResult
where
    T: serde::Serialize + Send + 'static,
    F: FnOnce(&Store) -> query::Result<T> + Send + 'static,
{
    match read_on(state.journal.clone(), f).await? {
        Read::Live(value) => ok(value),
        Read::Stale(snap) => ok(snap),
    }
}

type ApiResult = Result<Response, ApiError>;

fn ok<T: serde::Serialize>(v: T) -> ApiResult {
    Ok(Json(v).into_response())
}

async fn status(State(st): State<AppState>) -> ApiResult {
    let started = st.started;
    blocking(&st, move |s| query::status(s, Some(started))).await
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
    let journal = st.journal.clone();
    match read_on(journal.clone(), move |s| query::snapshot(s, params)).await? {
        Read::Live(snap) => {
            journal.remember(&snap);
            ok(snap)
        }
        Read::Stale(snap) => ok(snap),
    }
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
    blocking(&st, move |s| query::explain(s, params)).await
}

async fn metrics(
    State(st): State<AppState>,
    q: Result<Query<query::MetricsParams>, QueryRejection>,
) -> ApiResult {
    let Query(params) = q?;
    blocking(&st, move |s| query::metrics(s, params)).await
}

async fn history(
    State(st): State<AppState>,
    q: Result<Query<query::HistoryParams>, QueryRejection>,
) -> ApiResult {
    let Query(params) = q?;
    blocking(&st, move |s| query::history(s, params)).await
}

async fn search_events(
    State(st): State<AppState>,
    q: Result<Query<query::EventSearchParams>, QueryRejection>,
) -> ApiResult {
    let Query(params) = q?;
    blocking(&st, move |s| query::search(s, params)).await
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
    blocking(&st, move |s| query::changes(s, params)).await
}

async fn checkpoints(State(st): State<AppState>) -> ApiResult {
    blocking(&st, query::checkpoints).await
}

#[derive(Debug, Deserialize)]
struct FindingsQuery {
    status: Option<pulseblade_core::FindingStatus>,
    severity: Option<pulseblade_core::Severity>,
    limit: Option<usize>,
    detail: Option<bool>,
}

async fn findings(
    State(st): State<AppState>,
    q: Result<Query<FindingsQuery>, QueryRejection>,
) -> ApiResult {
    let Query(q) = q?;
    let params = query::FindingsParams {
        status: q.status,
        severity: q.severity,
        limit: q.limit,
        detail: q.detail,
    };
    blocking(&st, move |s| query::findings(s, params)).await
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
    resource: String,
    since: Option<String>,
    limit: Option<usize>,
}

async fn actions_list(State(st): State<AppState>) -> ApiResult {
    blocking(&st, |s| {
        Ok(serde_json::json!({ "runs": s.portal_actions(20)? }))
    })
    .await
}

enum Decision {
    Approve,
    Deny(String),
    Dismiss,
    Recheck,
}

#[derive(Debug, Deserialize)]
struct DenyBody {
    #[serde(default)]
    reason: String,
}

async fn approve_action(
    State(st): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult {
    decide_action(st, id, headers, Decision::Approve).await
}

async fn deny_action(
    State(st): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<Json<DenyBody>, JsonRejection>,
) -> ApiResult {
    let reason = match body {
        Ok(Json(body)) => body.reason,
        Err(_) => String::new(),
    };
    if reason.trim().is_empty() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "deny requires a reason".into(),
        ));
    }
    decide_action(st, id, headers, Decision::Deny(reason)).await
}

async fn dismiss_action(
    State(st): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult {
    decide_action(st, id, headers, Decision::Dismiss).await
}

async fn recheck_action(
    State(st): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult {
    decide_action(st, id, headers, Decision::Recheck).await
}

async fn decide_action(
    st: AppState,
    id: String,
    headers: HeaderMap,
    decision: Decision,
) -> ApiResult {
    if let Some(expected) = st.actions.approve_token() {
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");
        if !token_matches(expected, presented) {
            return Err(ApiError(
                StatusCode::UNAUTHORIZED,
                "approval token required".into(),
            ));
        }
    }
    // A hub row is not the run. The node that planned it applies the decision,
    // because that is the process that would start the command.
    if let Some(route) = st.route.clone() {
        let journal = st.journal.clone();
        let lookup = id.clone();
        let node = tokio::task::spawn_blocking(move || {
            let Some(store) = journal.store() else {
                return Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "database unavailable".into(),
                ));
            };
            match store.plan_node(&lookup) {
                Ok(node) => Ok(node),
                Err(err) if journal::is_down(&err) => {
                    journal.mark_down();
                    Err(ApiError(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "database unavailable".into(),
                    ))
                }
                Err(err) => Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
            }
        })
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;
        if node.is_some() {
            let run_id = id.clone();
            let run = tokio::task::spawn_blocking(move || match decision {
                Decision::Approve => route.approve(&run_id),
                Decision::Deny(reason) => route.deny(&run_id, &reason),
                Decision::Dismiss => route.dismiss(&run_id),
                Decision::Recheck => route.recheck(&run_id),
            })
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .map_err(route_approve_error)?;
            return ok(run);
        }
    }
    let actions = st.actions.clone();
    let journal = st.journal.clone();
    let run = tokio::task::spawn_blocking(move || {
        let Some(store) = journal.store() else {
            return Err(ApiError(
                StatusCode::SERVICE_UNAVAILABLE,
                "database unavailable".into(),
            ));
        };
        let result = match decision {
            Decision::Approve => actions.approve(&store, &id),
            Decision::Deny(reason) => actions.deny(&store, &id, &reason),
            Decision::Dismiss => actions.dismiss(&store, &id),
            Decision::Recheck => actions.recheck(&store, &id),
        };
        match result {
            Ok(run) => Ok(run),
            Err(pulseblade_act::ActError::Store(err)) if journal::is_down(&err) => {
                journal.mark_down();
                Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "database unavailable".into(),
                ))
            }
            Err(err) => Err(act_error(err)),
        }
    })
    .await
    .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;
    ok(run)
}

fn token_matches(expected: &str, presented: &str) -> bool {
    let a = expected.as_bytes();
    let b = presented.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

fn route_approve_error(err: String) -> ApiError {
    let lower = err.to_ascii_lowercase();
    let status = if lower.contains("did not answer") {
        StatusCode::BAD_GATEWAY
    } else if lower.contains("unknown run") || lower.contains("unknown action") {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_REQUEST
    };
    ApiError(status, err)
}

fn act_error(e: pulseblade_act::ActError) -> ApiError {
    let status = match &e {
        pulseblade_act::ActError::NotFound(_) => StatusCode::NOT_FOUND,
        pulseblade_act::ActError::Invalid(_) => StatusCode::BAD_REQUEST,
        pulseblade_act::ActError::Store(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ApiError(status, e.to_string())
}

async fn logs(
    State(st): State<AppState>,
    q: Result<Query<LogsQuery>, QueryRejection>,
) -> ApiResult {
    let Query(q) = q?;
    let params = query::LogsParams {
        resource: q.resource,
        since: q.since,
        limit: q.limit,
    };
    blocking(&st, move |s| query::logs(s, params)).await
}

async fn favicon() -> Response {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        FAVICON_PNG,
    )
        .into_response()
}

async fn prometheus(State(st): State<AppState>) -> ApiResult {
    match read_on(st.journal.clone(), prom::render).await? {
        Read::Live(text) => {
            Ok(([(header::CONTENT_TYPE, prom::CONTENT_TYPE)], text).into_response())
        }
        Read::Stale(snap) => ok(snap),
    }
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
    use pulseblade_core::{CollectorRun, Health, Observation, Resource, ResourceKind, Sample};
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
                        logs: vec![],
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
        assert!(v.get("nodes").is_none(), "a node has no cursor rows");
    }

    #[tokio::test]
    async fn status_reports_hub_node_push_age() {
        use pulseblade_core::{Change, ChangeKind};
        use pulseblade_store::NodeBatch;

        let store = Store::open_in_memory().unwrap();
        store
            .ingest_node(
                "web1",
                &NodeBatch {
                    changes: vec![Change {
                        seq: 1,
                        ts: Utc::now(),
                        resource_id: "host:web1".into(),
                        kind: ChangeKind::Appeared,
                        field: None,
                        before: None,
                        after: None,
                    }],
                    resources: vec![
                        Resource::new("host:web1", ResourceKind::Host, "web1").health(Health::Ok)
                    ],
                    ..Default::default()
                },
            )
            .unwrap();
        let app = router(Arc::new(store), Utc::now());
        let (code, v) = get_json(&app, "/api/v1/status").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["nodes"][0]["node"], "web1");
        assert_eq!(v["nodes"][0]["seq"], 1);
        assert!(v["nodes"][0]["last_push_age_secs"].as_i64().unwrap() < 30);
        assert_eq!(v["nodes"][0]["stale"], false);
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
        assert!(html.contains("/api/v1/history"));
        assert!(html.contains("/api/v1/search"));
        assert!(html.contains("Overview"));
        assert!(html.contains("Fleet"));
        assert!(html.contains("Add a client"));
        assert!(html.contains("deploy/k8s/daemonset.yaml"));
        assert!(html.contains("docs/DEPLOY.md"));
        assert!(html.contains("docs/CONFIG.md"));
        assert!(html.contains("data-theme-choice=\"light\""));
        assert!(html.contains("rel=\"icon\""));
        assert!(html.contains("name=\"pulseblade-time-format\" content=\"12h\""));
        assert!(html.contains("Time zone<select"));
        assert!(html.contains("Time format<select"));
        assert!(html.contains("12-hour (AM/PM)"));
        assert!(html.contains("24-hour"));
        assert!(html.contains("\"Restart window\""));
        assert!(html.contains("\"Lifetime restarts\""));
        assert!(html.contains("\"CPU share\""));
        assert!(html.contains("\"Memory warning\""));
        assert!(html.contains("\"Memory critical\""));
        assert!(html.contains("How recently a pod must have restarted to count as degraded"));
        assert!(html.contains("Degrade a running pod when the total reaches this"));
        assert!(html.contains("Percent of the host"));
        assert!(html.contains("Percent of RAM"));
        assert!(!html.contains("hour12: false"));

        let res = app
            .clone()
            .oneshot(
                HttpRequest::get("/favicon.ico")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("image/png")
        );
        let icon = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        assert!(icon.starts_with(b"\x89PNG"));
    }

    #[tokio::test]
    async fn portal_saves_health_and_rejects_policy() {
        let app = app();
        let (code, v) = get_json(&app, "/api/v1/portal").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["timezone"], "America/Los_Angeles");
        assert_eq!(v["time_format"], "12h");
        assert_eq!(v["health"]["pod_restart_window_mins"], 15);
        assert_eq!(v["health"]["pod_restart_lifetime"], 100);
        assert_eq!(v["health"]["proc_cpu_share_failed"], 90.0);
        assert_eq!(v["health"]["proc_mem_degraded"], 25.0);
        assert_eq!(v["health"]["proc_mem_failed"], 50.0);

        let saved = json!({
            "health": {
                "pod_restart_window_mins": 20,
                "pod_restart_lifetime": 100,
                "proc_cpu_share_failed": 90.0,
                "proc_mem_degraded": 25.0,
                "proc_mem_failed": 50.0
            }
        });
        let res = app
            .clone()
            .oneshot(
                HttpRequest::put("/api/v1/portal")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(saved.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["health"]["pod_restart_window_mins"], 20);

        let rejected = json!({
            "health": {
                "pod_restart_window_mins": 15,
                "tier": "auto"
            }
        });
        let res = app
            .clone()
            .oneshot(
                HttpRequest::put("/api/v1/portal")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(rejected.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let policy = json!({"actions": {"script_run": {"tier": "auto"}}, "health": {"pod_restart_window_mins": 15}});
        let res = app
            .clone()
            .oneshot(
                HttpRequest::put("/api/v1/portal")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(policy.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let format_body = json!({
            "time_format": "24h",
            "health": {
                "pod_restart_window_mins": 15,
                "pod_restart_lifetime": 100,
                "proc_cpu_share_failed": 90.0,
                "proc_mem_degraded": 25.0,
                "proc_mem_failed": 50.0
            }
        });
        let res = app
            .clone()
            .oneshot(
                HttpRequest::put("/api/v1/portal")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let (code, v) = get_json(&app, "/api/v1/portal").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["health"]["pod_restart_window_mins"], 20);
        assert_eq!(v["time_format"], "12h");
    }

    #[tokio::test]
    async fn portal_time_format_comes_from_toml_defaults() {
        let store = Store::open_in_memory().unwrap();
        let app = router_with_journal(
            journal::Journal::always(Arc::new(store)),
            Utc::now(),
            Arc::new(pulseblade_act::Registry::builtin()),
            PortalDefaults {
                timezone: "UTC".into(),
                time_format: "24h".into(),
                health: HealthThresholds::default(),
            },
            None,
        );
        let (code, v) = get_json(&app, "/api/v1/portal").await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(v["timezone"], "UTC");
        assert_eq!(v["time_format"], "24h");
        let (code, html, _) = get(&app, "/").await;
        assert_eq!(code, StatusCode::OK);
        assert!(html.contains("name=\"pulseblade-timezone\" content=\"UTC\""));
        assert!(html.contains("name=\"pulseblade-time-format\" content=\"24h\""));
    }

    #[test]
    fn invalid_portal_clock_defaults_fall_back() {
        let portal = PortalDefaults {
            timezone: "not a zone".into(),
            time_format: "hours".into(),
            health: HealthThresholds::default(),
        };
        assert_eq!(dashboard_timezone(&portal), "America/Los_Angeles");
        assert_eq!(dashboard_time_format(&portal), "12h");
    }

    #[test]
    fn dashboard_clock_renders_meridiem_only_for_12h() {
        let start = DASHBOARD.find("/* pb-time */").expect("clock helpers");
        let end = DASHBOARD.find("/* /pb-time */").expect("clock helpers end");
        let body = &DASHBOARD[start + "/* pb-time */".len()..end];
        let script = format!(
            r#"
            {body}
            const ts = "2026-10-26T20:15:00Z";
            const zone = "America/Los_Angeles";
            const norm = (s) => s.replace(/\s+/g, " ").trim();
            const c12 = norm(formatClock(ts, zone, "12h", false));
            const c24 = norm(formatClock(ts, zone, "24h", false));
            const s12 = norm(formatStamp(ts, zone, "12h"));
            const s24 = norm(formatStamp(ts, zone, "24h"));
            const sec12 = norm(formatClock(ts, zone, "12h", true));
            const sec24 = norm(formatClock(ts, zone, "24h", true));
            const meridiem = (s) => /AM|PM/.test(s);
            if (chosenFormat(null, null) !== "12h") throw new Error("fresh default " + chosenFormat(null, null));
            if (chosenFormat(undefined, "nope") !== "12h") throw new Error("bad server default");
            if (chosenFormat(null, "24h") !== "24h") throw new Error("toml default lost");
            if (chosenFormat("24h", "12h") !== "24h") throw new Error("browser choice lost");
            if (chosenFormat("12h", "24h") !== "12h") throw new Error("browser 12h lost");
            if (!hour12For("12h") || hour12For("24h")) throw new Error("hour12 flag");
            if (!meridiem(c12) || !meridiem(s12) || !meridiem(sec12)) {{
              throw new Error("12h missing AM/PM " + JSON.stringify({{c12, s12, sec12}}));
            }}
            if (meridiem(c24) || meridiem(s24) || meridiem(sec24)) {{
              throw new Error("24h has AM/PM " + JSON.stringify({{c24, s24, sec24}}));
            }}
            if (c12 !== "01:15 PM" || sec12 !== "01:15:00 PM") throw new Error("12h clock " + c12 + " " + sec12);
            if (c24 !== "13:15" || sec24 !== "13:15:00") throw new Error("24h clock " + c24 + " " + sec24);
            if (!s12.endsWith("PM") || s24.includes("PM") || s24.includes("AM")) {{
              throw new Error("stamp " + s12 + " | " + s24);
            }}
            "#
        );
        let output = std::process::Command::new("node")
            .arg("-e")
            .arg(&script)
            .output()
            .expect("node is required to check dashboard clock formatting");
        assert!(
            output.status.success(),
            "stdout {}\nstderr {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn dashboard_approve_posts_to_the_action_route() {
        assert!(DASHBOARD.contains(">Approve</button>"));
        assert!(DASHBOARD.contains(">Deny</button>"));
        assert!(DASHBOARD.contains(">Dismiss</button>"));
        assert!(DASHBOARD.contains(">Recheck</button>"));
        assert!(DASHBOARD.contains("Reason"));
        assert!(DASHBOARD.contains("data-approve"));
        assert!(DASHBOARD.contains("data-deny"));
        assert!(DASHBOARD.contains("data-dismiss"));
        assert!(DASHBOARD.contains("data-recheck"));
        assert!(DASHBOARD.contains("data-reason"));
        assert!(DASHBOARD.contains("/api/v1/actions/"));
        assert!(DASHBOARD.contains("method: \"POST\""));
        assert!(!DASHBOARD.contains("read-only"));
        assert_eq!(
            DASHBOARD.matches("decisionControl(").count(),
            4,
            "buttons on the actions tab, the finding, and the waiting callout"
        );
        let start = DASHBOARD.rfind("<script>").expect("dashboard script");
        let end = DASHBOARD.rfind("</script>").expect("dashboard script end");
        let script = &DASHBOARD[start + "<script>".len()..end];
        let path = std::env::temp_dir().join(format!("pulseblade-dash-{}.js", std::process::id()));
        std::fs::write(&path, script).unwrap();
        let output = std::process::Command::new("node")
            .arg("--check")
            .arg(&path)
            .output()
            .expect("node is required to check the dashboard script");
        let _ = std::fs::remove_file(&path);
        assert!(
            output.status.success(),
            "stdout {}\nstderr {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
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
