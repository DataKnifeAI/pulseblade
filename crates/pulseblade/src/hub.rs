//! Hub: nodes connect out to push their journal and long-poll for actions.
//!
//! The hub copies journal rows into fleet state and serves the same MCP tools.
//! Action calls are routed to the node that owns the resource. That node runs
//! them through its own registry, so a hub cannot bypass approve or deny.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use pulseblade_act::Registry;
use pulseblade_core::{host_of_id, HealthThresholds};
use pulseblade_mcp::journal::{self, Journal, WriteError};
use pulseblade_mcp::query::{self, SnapshotParams};
use pulseblade_mcp::{build_router, ActionRoute, PortalDefaults};
use pulseblade_store::{
    ChangeFilter, IngestResult, NodeBatch, NodeCursor, RouteJob, Store, StoreError,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::config::HubTls;

pub struct HubInner {
    journal: Arc<Journal>,
    /// Node id to bearer token.
    tokens: Vec<(String, String)>,
    waiters: Mutex<HashMap<String, mpsc::Sender<Result<Value, String>>>>,
    notify: tokio::sync::Notify,
    action_wait: Duration,
    ids: AtomicU64,
    /// Toml cutoffs used when the hub has not stored a portal document.
    health: Mutex<HealthThresholds>,
}

impl HubInner {
    fn node_for_token(&self, presented: &str) -> Option<String> {
        let mut found = None;
        for (id, expected) in &self.tokens {
            if ct_eq(expected.as_bytes(), presented.as_bytes()) {
                found = Some(id.clone());
            }
        }
        found
    }

    fn next_id(&self) -> String {
        let n = self.ids.fetch_add(1, Ordering::Relaxed);
        format!("r{n}-{}", chrono::Utc::now().timestamp_millis())
    }

    fn call(&self, node_id: &str, op: &str, body: Value) -> Result<Value, String> {
        let id = self.next_id();
        let (tx, rx) = mpsc::channel();
        self.waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.clone(), tx);
        self.with_store(|store| store.enqueue_route(&id, node_id, op, &body))?;
        self.notify.notify_waiters();
        match rx.recv_timeout(self.action_wait) {
            Ok(result) => result,
            Err(_) => {
                self.waiters
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id);
                Err(format!("node {node_id} did not answer"))
            }
        }
    }

    fn annotate_run(
        &self,
        run: pulseblade_core::ActionRun,
    ) -> Result<pulseblade_core::ActionRun, String> {
        let remembered = self.with_store(|store| store.plan_finding(&run.id))?;
        let run = remember_finding(run, remembered);
        if run.finding_outcome.is_some() {
            if run.finding_outcome == Some(pulseblade_core::FindingOutcome::Resolved) {
                self.with_store(|store| store.learn_remedy(&run))?;
            }
            return Ok(run);
        }
        self.with_store(|store| store.attach_finding_outcome(run))
    }

    fn with_store<T>(&self, f: impl FnOnce(&Store) -> Result<T, StoreError>) -> Result<T, String> {
        self.journal
            .try_store(f)
            .map_err(|err| write_error(&self.journal, err))
    }
}

fn write_error(journal: &Journal, err: WriteError) -> String {
    match err {
        WriteError::Unavailable => "database unavailable".into(),
        WriteError::Store(err) if journal::is_down(&err) => {
            journal.mark_down();
            "database unavailable".into()
        }
        WriteError::Store(err) => err.to_string(),
    }
}

impl ActionRoute for HubInner {
    fn plan(
        &self,
        params: pulseblade_act::PlanParams,
    ) -> Result<pulseblade_act::PlanResult, String> {
        let finding_id = params.finding_id.clone();
        let resource_id = params.resource_id.clone();
        let store = self
            .journal
            .store()
            .ok_or_else(|| "database unavailable".to_string())?;
        let node = route_node(&store, params.resource_id.as_deref(), &self.tokens)?;
        let value = self.call(
            &node,
            "plan",
            serde_json::to_value(&params).map_err(|e| e.to_string())?,
        )?;
        let mut planned: pulseblade_act::PlanResult =
            serde_json::from_value(value).map_err(|e| e.to_string())?;
        if planned.finding_id.is_none() {
            planned.finding_id = finding_id;
        }
        self.with_store(|store| {
            store.remember_plan(
                &planned.plan_id,
                &node,
                planned.finding_id.as_deref(),
                resource_id.as_deref(),
            )
        })?;
        Ok(planned)
    }

    fn execute(
        &self,
        params: pulseblade_act::ExecuteParams,
    ) -> Result<pulseblade_core::ActionRun, String> {
        let node = self
            .with_store(|store| store.plan_node(&params.plan_id))?
            .ok_or_else(|| format!("unknown plan {}", params.plan_id))?;
        let value = self.call(
            &node,
            "execute",
            serde_json::to_value(&params).map_err(|e| e.to_string())?,
        )?;
        let run: pulseblade_core::ActionRun =
            serde_json::from_value(value).map_err(|e| e.to_string())?;
        let run = self.annotate_run(run).map_err(|e| e.to_string())?;
        let body = serde_json::to_value(&run).map_err(|e| e.to_string())?;
        self.with_store(|store| store.cache_run(&run.id, &node, &body))?;
        Ok(run)
    }

    fn status(
        &self,
        params: pulseblade_act::StatusParams,
    ) -> Result<pulseblade_core::ActionRun, String> {
        let tied = self.with_store(|store| store.plan_finding(&params.run_id))?;
        // A finished run's output does not change. A finding tied to it does:
        // approval can still be waiting, then the finding can clear.
        if tied.is_none() {
            if let Some((_, body)) = self.with_store(|store| store.cached_run(&params.run_id))? {
                return serde_json::from_value(body).map_err(|e| e.to_string());
            }
        }
        let node = self
            .with_store(|store| store.plan_node(&params.run_id))?
            .ok_or_else(|| format!("unknown run {}", params.run_id))?;
        match self.call(
            &node,
            "status",
            serde_json::to_value(&params).map_err(|e| e.to_string())?,
        ) {
            Ok(value) => {
                let run: pulseblade_core::ActionRun =
                    serde_json::from_value(value).map_err(|e| e.to_string())?;
                let run = self.annotate_run(run).map_err(|e| e.to_string())?;
                let body = serde_json::to_value(&run).map_err(|e| e.to_string())?;
                self.with_store(|store| store.cache_run(&run.id, &node, &body))?;
                Ok(run)
            }
            Err(err) => {
                if let Some((_, body)) =
                    self.with_store(|store| store.cached_run(&params.run_id))?
                {
                    let run: pulseblade_core::ActionRun =
                        serde_json::from_value(body).map_err(|e| e.to_string())?;
                    let mut run = remember_finding(run, tied);
                    run.finding_outcome = None;
                    return self.with_store(|store| store.attach_finding_outcome(run));
                }
                Err(err)
            }
        }
    }

    fn approve(&self, run_id: &str) -> Result<pulseblade_core::ActionRun, String> {
        let node = self
            .with_store(|store| store.plan_node(run_id))?
            .ok_or_else(|| format!("unknown run {run_id}"))?;
        let value = self.call(&node, "approve", serde_json::json!({ "run_id": run_id }))?;
        let run: pulseblade_core::ActionRun =
            serde_json::from_value(value).map_err(|e| e.to_string())?;
        let run = self.annotate_run(run)?;
        let body = serde_json::to_value(&run).map_err(|e| e.to_string())?;
        self.with_store(|store| store.cache_run(&run.id, &node, &body))?;
        Ok(run)
    }

    fn deny(&self, run_id: &str) -> Result<pulseblade_core::ActionRun, String> {
        let node = self
            .with_store(|store| store.plan_node(run_id))?
            .ok_or_else(|| format!("unknown run {run_id}"))?;
        let value = self.call(&node, "deny", serde_json::json!({ "run_id": run_id }))?;
        let run: pulseblade_core::ActionRun =
            serde_json::from_value(value).map_err(|e| e.to_string())?;
        let run = self.annotate_run(run)?;
        let body = serde_json::to_value(&run).map_err(|e| e.to_string())?;
        self.with_store(|store| store.cache_run(&run.id, &node, &body))?;
        Ok(run)
    }
}

/// Keep a node-computed outcome. Fill the finding id the hub remembered, and
/// compute the outcome here when the node did not (an older node drops the field).
fn remember_finding(
    mut run: pulseblade_core::ActionRun,
    finding_id: Option<String>,
) -> pulseblade_core::ActionRun {
    if run.finding_id.as_ref().is_none_or(|id| id.is_empty()) {
        run.finding_id = finding_id.filter(|id| !id.is_empty());
    }
    run
}

fn route_node(
    store: &Store,
    resource_id: Option<&str>,
    tokens: &[(String, String)],
) -> Result<String, String> {
    let id = resource_id.ok_or("resource_id is required so the hub can route to a node")?;
    let host = host_of_id(id).unwrap_or("");
    // `unit:<node>:…` puts the enrolled node in the second segment.
    // `workload:<cluster>:…` and `pod:<cluster>:…` put the cluster there.
    if !host.is_empty() && tokens.iter().any(|(node, _)| node == host) {
        return Ok(host.to_string());
    }
    let row = store.resource(id).map_err(|e| e.to_string())?;
    if let Some(owner) = row.as_ref().and_then(|row| owner_node(&row.source)) {
        if tokens.iter().any(|(node, _)| node == owner) {
            return Ok(owner.to_string());
        }
        return Err(format!(
            "resource `{id}` was reported by {owner}, which is not enrolled"
        ));
    }
    if host.is_empty() {
        Err(format!("resource id `{id}` has no host"))
    } else {
        Err(format!("no enrolled node named {host}"))
    }
}

/// Hub ingest records the pusher as `node:<id>`.
fn owner_node(source: &str) -> Option<&str> {
    let id = source.strip_prefix("node:")?;
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= (x ^ y) as usize;
    }
    diff == 0
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
}

fn authorize(hub: &HubInner, headers: &HeaderMap) -> Result<String, StatusCode> {
    let token = bearer(headers).ok_or(StatusCode::UNAUTHORIZED)?;
    hub.node_for_token(token).ok_or(StatusCode::UNAUTHORIZED)
}

/// Changes, present resources, and samples the node has past `cursor`.
///
/// When the hub cursor is ahead of this database, the journal was replaced
/// (the DaemonSet keeps SQLite on an emptyDir, so a rollout starts seq at 1).
/// Replaying from the stored cursor would drop every new change, and the
/// resource bodies that travel with those changes, while samples still move.
pub fn batch_since(store: &Store, cursor: NodeCursor) -> Result<NodeBatch, StoreError> {
    let local_seq = store.current_seq()?;
    let journal_reset = cursor.seq > local_seq;
    let change_after = if journal_reset { 0 } else { cursor.seq };
    let (changes, _) = store.changes_after(change_after, &ChangeFilter::default(), 2_000)?;
    let mut seen = std::collections::BTreeSet::new();
    let mut resources = Vec::new();
    for change in &changes {
        if seen.insert(change.resource_id.clone()) {
            if let Some(row) = store.resource(&change.resource_id)? {
                if row.present {
                    resources.push(row.resource);
                }
            }
        }
    }
    let samples = store.samples_since(cursor.sample_ms, 2_000)?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let open = (now_ms / pulseblade_store::BUCKET_MS) * pulseblade_store::BUCKET_MS;
    // Closed buckets only. The open window is still arriving as raw samples.
    let buckets = store.closed_buckets_since(cursor.bucket_ms, open, 2_000)?;
    // Error lines, failures, and restarts. Info logs stay on the node.
    let events = store.events_after(cursor.event_id, 2_000)?;
    Ok(NodeBatch {
        changes,
        resources,
        samples,
        buckets,
        events,
        journal_reset,
        ..NodeBatch::default()
    })
}

/// Run one routed call on the node. Policy is this registry, not the hub's.
pub fn apply_job(reg: &Registry, store: &Store, job: &RouteJob) -> Result<Value, String> {
    match job.op.as_str() {
        "plan" => {
            let mut params: pulseblade_act::PlanParams =
                serde_json::from_value(job.body.clone()).map_err(|e| e.to_string())?;
            if params.idempotency_key.is_none() {
                params.idempotency_key = Some(job.id.clone());
            }
            let planned = reg.plan(store, params).map_err(|e| e.to_string())?;
            serde_json::to_value(&planned).map_err(|e| e.to_string())
        }
        "execute" => {
            let params: pulseblade_act::ExecuteParams =
                serde_json::from_value(job.body.clone()).map_err(|e| e.to_string())?;
            let run = reg
                .execute(store, &params.plan_id)
                .map_err(|e| e.to_string())?;
            serde_json::to_value(&run).map_err(|e| e.to_string())
        }
        "status" => {
            let params: pulseblade_act::StatusParams =
                serde_json::from_value(job.body.clone()).map_err(|e| e.to_string())?;
            let run = reg
                .status(store, &params.run_id)
                .map_err(|e| e.to_string())?;
            serde_json::to_value(&run).map_err(|e| e.to_string())
        }
        "approve" => {
            let params: pulseblade_act::StatusParams =
                serde_json::from_value(job.body.clone()).map_err(|e| e.to_string())?;
            let run = reg
                .approve(store, &params.run_id)
                .map_err(|e| e.to_string())?;
            serde_json::to_value(&run).map_err(|e| e.to_string())
        }
        "deny" => {
            let params: pulseblade_act::StatusParams =
                serde_json::from_value(job.body.clone()).map_err(|e| e.to_string())?;
            let run = reg.deny(store, &params.run_id).map_err(|e| e.to_string())?;
            serde_json::to_value(&run).map_err(|e| e.to_string())
        }
        other => Err(format!("unknown route op {other}")),
    }
}

#[cfg(test)]
pub fn new_hub(
    store: Arc<Store>,
    tokens: Vec<(String, String)>,
    action_wait: Duration,
) -> Arc<HubInner> {
    new_hub_journal(Journal::always(store), tokens, action_wait)
}

pub fn new_hub_journal(
    journal: Arc<Journal>,
    tokens: Vec<(String, String)>,
    action_wait: Duration,
) -> Arc<HubInner> {
    Arc::new(HubInner {
        journal,
        tokens,
        waiters: Mutex::new(HashMap::new()),
        notify: tokio::sync::Notify::new(),
        action_wait,
        ids: AtomicU64::new(1),
        health: Mutex::new(HealthThresholds::default()),
    })
}

impl HubInner {
    /// Cutoffs from the hub's toml. A stored portal document still wins on poll.
    pub fn set_health_defaults(&self, health: HealthThresholds) {
        *self.health.lock().unwrap_or_else(|e| e.into_inner()) = health;
    }
}

pub async fn serve(
    listener: TcpListener,
    hub: Arc<HubInner>,
    allowed_hosts: Vec<String>,
    shutdown: CancellationToken,
    actions: Arc<Registry>,
    tls: Option<Arc<rustls::ServerConfig>>,
    portal: PortalDefaults,
) -> anyhow::Result<()> {
    let journal = hub.journal.clone();
    let extra = Router::new()
        .route("/hub/v1/cursor", get(cursor))
        .route("/hub/v1/push", post(push))
        .route("/hub/v1/poll", get(poll))
        .route("/hub/v1/result", post(result))
        .with_state(hub.clone());
    let router = build_router(
        journal,
        allowed_hosts,
        shutdown.clone(),
        actions,
        Some(hub),
        extra,
        portal,
    );
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "hub serving");
    if let Some(config) = tls {
        let listener = TlsListen {
            inner: listener,
            acceptor: tokio_rustls::TlsAcceptor::from(config),
        };
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await?;
    } else {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await?;
    }
    Ok(())
}

/// How the hub opens its journal. Retried in the background; a failure does not
/// stop the listener.
#[derive(Clone)]
pub enum DbOpener {
    Postgres { url: String, ca: Option<String> },
    Sqlite(std::path::PathBuf),
}

impl DbOpener {
    pub fn open(&self) -> anyhow::Result<Store> {
        match self {
            Self::Postgres { url, ca } => Store::open_postgres(url, ca.as_deref())
                .context("opening the hub postgres database"),
            Self::Sqlite(path) => {
                if let Some(dir) = path.parent() {
                    std::fs::create_dir_all(dir)
                        .with_context(|| format!("creating {}", dir.display()))?;
                }
                Store::open(path).with_context(|| format!("opening {}", path.display()))
            }
        }
    }
}

fn note_down(journal: &Journal, err: &StoreError) {
    if journal::is_down(err) {
        journal.mark_down();
    }
}

/// Retry the pool, then keep offline findings and retention while it is up.
/// A successful summary read refreshes the snapshot cache.
pub async fn supervise(
    journal: Arc<Journal>,
    opener: DbOpener,
    shutdown: CancellationToken,
    history_days: u64,
    change_days: u64,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut next_maintain = tokio::time::Instant::now();
    let mut next_prune = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => {
                if !journal.is_up() {
                    let opener = opener.clone();
                    match tokio::task::spawn_blocking(move || opener.open()).await {
                        Ok(Ok(store)) => journal.install(Arc::new(store)),
                        Ok(Err(err)) => {
                            tracing::warn!(error = %err, "hub database unavailable; retrying");
                        }
                        Err(err) => tracing::warn!(error = %err, "hub database unavailable; retrying"),
                    }
                    if !journal.is_up() {
                        continue;
                    }
                }
                if tokio::time::Instant::now() < next_maintain {
                    continue;
                }
                next_maintain = tokio::time::Instant::now() + Duration::from_secs(15);
                let Some(store) = journal.store() else { continue };
                let checked = store.clone();
                match tokio::task::spawn_blocking(move || checked.sync_offline_hosts(chrono::Utc::now())).await {
                    Ok(Ok(opened)) => {
                        for finding in opened {
                            tracing::info!(
                                id = %finding.id,
                                resource = %finding.resource_id,
                                summary = %finding.summary,
                                "finding"
                            );
                        }
                    }
                    Ok(Err(err)) => {
                        note_down(&journal, &err);
                        tracing::warn!(error = %err, "offline check failed");
                    }
                    Err(err) => tracing::warn!(error = %err, "offline check failed"),
                }
                let refreshed = journal.clone();
                match tokio::task::spawn_blocking(move || {
                    let Some(store) = refreshed.store() else {
                        return Ok(());
                    };
                    match query::snapshot(
                        &store,
                        SnapshotParams {
                            detail: Some(query::Detail::Summary),
                            ..Default::default()
                        },
                    ) {
                        Ok(snap) => {
                            refreshed.remember(&snap);
                            Ok(())
                        }
                        Err(query::QueryError::Store(err)) => Err(err),
                        Err(err) => Err(StoreError::Invalid(err.to_string())),
                    }
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        note_down(&journal, &err);
                        tracing::warn!(error = %err, "snapshot cache refresh failed");
                    }
                    Err(err) => tracing::warn!(error = %err, "snapshot cache refresh failed"),
                }
                if tokio::time::Instant::now() >= next_prune {
                    let Some(pruned) = journal.store() else { continue };
                    let history_days = history_days.max(1);
                    let change_days = change_days.max(1);
                    match tokio::task::spawn_blocking(move || {
                        let now = chrono::Utc::now();
                        pruned.prune(
                            now - chrono::Duration::hours(pulseblade_store::HUB_RAW_HOURS),
                            now - chrono::Duration::days(change_days as i64),
                            now - chrono::Duration::days(history_days as i64),
                        )
                    })
                    .await
                    {
                        Ok(Ok((samples, changes))) => {
                            tracing::info!(samples, changes, "pruned");
                        }
                        Ok(Err(err)) => {
                            note_down(&journal, &err);
                            tracing::warn!(error = %err, "hub prune failed");
                        }
                        Err(err) => tracing::warn!(error = %err, "hub prune failed"),
                    }
                    next_prune = tokio::time::Instant::now() + Duration::from_secs(3600);
                }
            }
        }
    }
}

struct TlsListen {
    inner: TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
}

impl axum::serve::Listener for TlsListen {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.inner.accept().await {
                Ok((stream, addr)) => match self.acceptor.accept(stream).await {
                    Ok(tls) => return (tls, addr),
                    Err(e) => {
                        tracing::debug!(error = %e, "tls handshake failed");
                    }
                },
                Err(e) => {
                    tracing::debug!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

fn install_crypto() {
    // rustls is built with more than one crypto provider in this workspace.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub fn server_config(tls: &HubTls) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    install_crypto();
    if tls.cert.is_empty() || tls.key.is_empty() || tls.client_ca.is_empty() {
        anyhow::bail!("[hub.tls] needs cert, key, and client_ca together");
    }
    let certs = load_certs(&tls.cert)?;
    let key = load_key(&tls.key)?;
    let mut roots = rustls::RootCertStore::empty();
    for ca in load_certs(&tls.client_ca)? {
        roots.add(ca).context("reading client CA")?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .context("building client verifier")?;
    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("building hub tls config")?;
    Ok(Arc::new(config))
}

fn load_certs(path: &str) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {path}"))?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("reading certs in {path}"))
}

fn load_key(path: &str) -> anyhow::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {path}"))?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("reading key in {path}"))?
        .ok_or_else(|| anyhow::anyhow!("no private key in {path}"))
}

fn unavailable(err: &WriteError) -> bool {
    matches!(err, WriteError::Unavailable)
}

async fn cursor(
    State(hub): State<Arc<HubInner>>,
    headers: HeaderMap,
) -> Result<Json<NodeCursor>, StatusCode> {
    let node = authorize(&hub, &headers)?;
    let journal = hub.journal.clone();
    match tokio::task::spawn_blocking(move || journal.try_store(|store| store.node_cursor(&node)))
        .await
    {
        Ok(Ok(cursor)) => Ok(Json(cursor)),
        Ok(Err(err)) if unavailable(&err) => Err(StatusCode::SERVICE_UNAVAILABLE),
        _ => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn push(
    State(hub): State<Arc<HubInner>>,
    headers: HeaderMap,
    Json(batch): Json<NodeBatch>,
) -> Result<Json<IngestResult>, (StatusCode, String)> {
    let node = authorize(&hub, &headers).map_err(|s| (s, "unauthorized".into()))?;
    let journal = hub.journal.clone();
    let result = tokio::task::spawn_blocking(move || {
        journal.try_store(|store| store.ingest_node(&node, &batch))
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| match e {
        WriteError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "database unavailable".into(),
        ),
        WriteError::Store(err @ StoreError::Gap { .. }) => (StatusCode::CONFLICT, err.to_string()),
        WriteError::Store(err) => (StatusCode::BAD_REQUEST, err.to_string()),
    })?;
    Ok(Json(result))
}

/// Health document beside the routed jobs. Omitted when the store cannot be read,
/// so a node keeps the last good copy instead of replacing it with defaults.
fn poll_body(hub: &HubInner, jobs: Vec<Value>) -> Value {
    let mut payload = json!({ "jobs": jobs });
    let fallback = hub.health.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let Some(store) = hub.journal.store() else {
        return payload;
    };
    let health = match store.health_thresholds() {
        Ok(Some(doc)) => Some(doc),
        Ok(None) => Some(fallback),
        Err(err) => {
            tracing::warn!(error = %err, "health thresholds unreadable; poll omits them");
            None
        }
    };
    if let Some(health) = health {
        if let Ok(value) = serde_json::to_value(&health) {
            payload["health"] = value;
        }
    }
    payload
}

#[derive(Deserialize)]
struct PollQuery {
    #[serde(default)]
    wait_ms: u64,
}

async fn poll(
    State(hub): State<Arc<HubInner>>,
    headers: HeaderMap,
    Query(q): Query<PollQuery>,
) -> Result<Json<Value>, StatusCode> {
    let node = authorize(&hub, &headers)?;
    let wait = Duration::from_millis(q.wait_ms.min(30_000));
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let jobs = {
            let journal = hub.journal.clone();
            let node = node.clone();
            match tokio::task::spawn_blocking(move || {
                journal.try_store(|store| store.poll_routes(&node, 20))
            })
            .await
            {
                Ok(Ok(jobs)) => jobs,
                Ok(Err(err)) if unavailable(&err) => {
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
                _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            }
        };
        if !jobs.is_empty() || wait.is_zero() || tokio::time::Instant::now() >= deadline {
            let jobs = jobs
                .into_iter()
                .map(|j| json!({"id": j.id, "op": j.op, "body": j.body}))
                .collect::<Vec<_>>();
            return Ok(Json(poll_body(&hub, jobs)));
        }
        let notified = hub.notify.notified();
        tokio::pin!(notified);
        let again = {
            let journal = hub.journal.clone();
            let node = node.clone();
            match tokio::task::spawn_blocking(move || {
                journal.try_store(|store| store.poll_routes(&node, 1))
            })
            .await
            {
                Ok(Ok(jobs)) => jobs,
                Ok(Err(err)) if unavailable(&err) => {
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
                _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            }
        };
        if !again.is_empty() {
            continue;
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep_until(deadline) => {}
        }
    }
}

#[derive(Deserialize)]
struct ResultBody {
    id: String,
    ok: bool,
    body: Value,
}

async fn result(
    State(hub): State<Arc<HubInner>>,
    headers: HeaderMap,
    Json(body): Json<ResultBody>,
) -> Result<StatusCode, StatusCode> {
    let node = authorize(&hub, &headers)?;
    let value = if body.ok {
        body.body.clone()
    } else {
        json!({ "error": body.body })
    };
    let updated = {
        let journal = hub.journal.clone();
        let id = body.id.clone();
        let node = node.clone();
        match tokio::task::spawn_blocking(move || {
            journal.try_store(|store| store.finish_route(&id, &node, &value))
        })
        .await
        {
            Ok(Ok(updated)) => updated,
            Ok(Err(err)) if unavailable(&err) => return Err(StatusCode::SERVICE_UNAVAILABLE),
            _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        }
    };
    if !updated {
        return Err(StatusCode::NOT_FOUND);
    }
    if let Some(tx) = hub
        .waiters
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&body.id)
    {
        let msg = if body.ok {
            Ok(body.body)
        } else {
            Err(body
                .body
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("node rejected the call")
                .to_string())
        };
        let _ = tx.send(msg);
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Jobs from a hub long-poll, plus the health document when the hub sent one.
pub struct PollReply {
    pub jobs: Vec<RouteJob>,
    pub health: Option<HealthThresholds>,
}

/// Outbound client a node uses. Plain HTTP uses ureq. mTLS uses rustls.
#[derive(Clone)]
pub struct HubClient {
    base: String,
    token: String,
    agent: ureq::Agent,
    tls: Option<Arc<rustls::ClientConfig>>,
    interval_secs: u64,
}

impl HubClient {
    #[cfg(test)]
    pub fn plain(base: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            base: base.into().trim_end_matches('/').to_string(),
            token: token.into(),
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(40))
                .build(),
            tls: None,
            interval_secs: 15,
        }
    }

    pub fn cursor(&self) -> anyhow::Result<NodeCursor> {
        let v = self.exchange("GET", "/hub/v1/cursor", None)?;
        Ok(serde_json::from_value(v)?)
    }

    pub fn push(&self, batch: &NodeBatch) -> anyhow::Result<IngestResult> {
        let v = self.exchange("POST", "/hub/v1/push", Some(serde_json::to_value(batch)?))?;
        Ok(serde_json::from_value(v)?)
    }

    pub fn push_store(&self, store: &Store) -> anyhow::Result<IngestResult> {
        let cursor = self.cursor()?;
        let mut batch = batch_since(store, cursor)?;
        batch.interval_secs = self.interval_secs;
        if !batch.journal_reset
            && batch.changes.is_empty()
            && batch.samples.is_empty()
            && batch.buckets.is_empty()
            && batch.events.is_empty()
        {
            return Ok(IngestResult {
                cursor: cursor.seq,
                sample_ms: cursor.sample_ms,
                applied: 0,
            });
        }
        self.push(&batch)
    }

    /// One last push before exit. A crash never calls this, so the hub sees silence.
    pub fn push_shutdown(&self, store: &Store) -> anyhow::Result<IngestResult> {
        let cursor = self.cursor()?;
        let mut batch = batch_since(store, cursor)?;
        batch.interval_secs = self.interval_secs;
        batch.shutting_down = true;
        self.push(&batch)
    }

    pub fn poll(&self, wait_ms: u64) -> anyhow::Result<PollReply> {
        let v = self.exchange("GET", &format!("/hub/v1/poll?wait_ms={wait_ms}"), None)?;
        let jobs = v
            .get("jobs")
            .and_then(|j| j.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::new();
        for job in jobs {
            out.push(RouteJob {
                id: job["id"].as_str().unwrap_or_default().to_string(),
                op: job["op"].as_str().unwrap_or_default().to_string(),
                body: job["body"].clone(),
            });
        }
        let health = match v.get("health") {
            Some(raw) => match HealthThresholds::from_value(raw) {
                Ok(doc) => Some(doc),
                Err(err) => {
                    tracing::warn!(error = %err, "ignoring invalid hub health document");
                    None
                }
            },
            None => None,
        };
        Ok(PollReply { jobs: out, health })
    }

    pub fn finish(&self, id: &str, ok: bool, body: &Value) -> anyhow::Result<()> {
        let _ = self.exchange(
            "POST",
            "/hub/v1/result",
            Some(json!({"id": id, "ok": ok, "body": body})),
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub fn drain(&self, store: &Store, reg: &Registry) -> anyhow::Result<()> {
        for job in self.poll(0)?.jobs {
            match apply_job(reg, store, &job) {
                Ok(body) => self.finish(&job.id, true, &body)?,
                Err(e) => self.finish(&job.id, false, &json!({"error": e}))?,
            }
        }
        Ok(())
    }

    fn exchange(&self, method: &str, path: &str, body: Option<Value>) -> anyhow::Result<Value> {
        let url = format!("{}{path}", self.base);
        // Client certificates are mutual TLS. An https:// URL without them uses
        // ureq, which verifies the server certificate and still sends the bearer token.
        if let Some(tls) = &self.tls {
            anyhow::ensure!(
                self.base.starts_with("https://"),
                "upstream client certificate requires an https:// hub url"
            );
            return tls_exchange(tls, &url, &self.token, method, body);
        }
        let auth = format!("Bearer {}", self.token);
        let response = match (method, body) {
            ("GET", _) => self.agent.get(&url).set("Authorization", &auth).call(),
            ("POST", Some(body)) => self
                .agent
                .post(&url)
                .set("Authorization", &auth)
                .send_json(body),
            ("POST", None) => self
                .agent
                .post(&url)
                .set("Authorization", &auth)
                .send_json(json!({})),
            _ => anyhow::bail!("unsupported method {method}"),
        };
        let response = match response {
            Ok(r) => r,
            Err(ureq::Error::Status(code, r)) => {
                let text = r.into_string().unwrap_or_default();
                anyhow::bail!("hub {method} {path} returned {code}: {text}");
            }
            Err(e) => return Err(e.into()),
        };
        if response.status() == 204 {
            return Ok(Value::Null);
        }
        Ok(response.into_json()?)
    }
}

/// Overall HTTP deadline shared by hub cursor, push, and poll.
///
/// A call still has to finish after sitting in the hub's serialized SQLite
/// queue. The deadline stays longer than the long-poll the client asks the
/// hub to hold (`poll_secs`, capped at 30s). It does not change that wait.
fn hub_client_timeout(poll_secs: u64) -> Duration {
    let wait_secs = poll_secs.clamp(1, 60).min(30);
    Duration::from_secs(30.max(wait_secs + 5))
}

/// Push the local journal and apply any routed actions until `shutdown`.
pub fn run_upstream(
    store: Arc<Store>,
    reg: Arc<Registry>,
    base: String,
    token: String,
    tls: Option<Arc<rustls::ClientConfig>>,
    poll_secs: u64,
    shutdown: CancellationToken,
) {
    let client = HubClient {
        base: base.trim_end_matches('/').to_string(),
        token,
        agent: ureq::AgentBuilder::new()
            .timeout(hub_client_timeout(poll_secs))
            .build(),
        tls,
        interval_secs: poll_secs.clamp(1, 60),
    };
    tracing::info!(url = %client.base, mtls = client.tls.is_some(), "pushing to hub");
    let pause = Duration::from_secs(poll_secs.clamp(1, 60));
    while !shutdown.is_cancelled() {
        if let Err(e) = client.push_store(&store) {
            tracing::warn!(error = %e, "hub push failed");
        }
        match client.poll(pause.as_millis().min(30_000) as u64) {
            Ok(reply) => {
                if let Some(health) = reply.health {
                    let changed =
                        store.health_thresholds().ok().flatten().as_ref() != Some(&health);
                    if changed {
                        if let Err(e) = store.set_health_thresholds(&health) {
                            tracing::warn!(error = %e, "saving hub health thresholds failed");
                        }
                    }
                }
                for job in reply.jobs {
                    match apply_job(&reg, &store, &job) {
                        Ok(body) => {
                            if let Err(e) = client.finish(&job.id, true, &body) {
                                tracing::warn!(error = %e, "hub result failed");
                            }
                        }
                        Err(e) => {
                            let _ = client.finish(&job.id, false, &json!({"error": e}));
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "hub poll failed");
                let step = Duration::from_millis(200);
                let mut left = pause;
                while left > Duration::ZERO && !shutdown.is_cancelled() {
                    std::thread::sleep(step.min(left));
                    left = left.saturating_sub(step);
                }
            }
        }
    }
    if let Err(e) = client.push_shutdown(&store) {
        tracing::warn!(error = %e, "shutdown push failed");
    }
}

pub fn client_config(ca: &str, cert: &str, key: &str) -> anyhow::Result<Arc<rustls::ClientConfig>> {
    install_crypto();
    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(ca)? {
        roots.add(c).context("reading hub CA")?;
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(load_certs(cert)?, load_key(key)?)
        .context("building node tls config")?;
    Ok(Arc::new(config))
}

fn tls_exchange(
    config: &Arc<rustls::ClientConfig>,
    url: &str,
    token: &str,
    method: &str,
    body: Option<Value>,
) -> anyhow::Result<Value> {
    let rest = url
        .strip_prefix("https://")
        .context("tls client only speaks https")?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let path = format!("/{path}");
    let (host, port) = authority
        .rsplit_once(':')
        .and_then(|(h, p)| p.parse().ok().map(|p| (h.to_string(), p)))
        .unwrap_or_else(|| (authority.to_string(), 443));
    let name = server_name(&host)?;
    let sock = std::net::TcpStream::connect((host.as_str(), port))
        .with_context(|| format!("connecting to {host}:{port}"))?;
    sock.set_read_timeout(Some(Duration::from_secs(40)))?;
    sock.set_write_timeout(Some(Duration::from_secs(40)))?;
    let conn = rustls::ClientConnection::new(config.clone(), name).context("tls client")?;
    let mut stream = rustls::StreamOwned::new(conn, sock);
    let payload = body.as_ref().map(|v| v.to_string()).unwrap_or_default();
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n"
    );
    if body.is_some() {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", payload.len()));
    }
    req.push_str("\r\n");
    req.push_str(&payload);
    stream.write_all(req.as_bytes())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let text = String::from_utf8_lossy(&raw);
    let (head, resp_body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("short tls response"))?;
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        anyhow::bail!("hub {method} {path} returned {status}: {resp_body}");
    }
    if status == 204 || resp_body.is_empty() {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_str(resp_body.trim())?)
}

fn server_name(host: &str) -> anyhow::Result<rustls::pki_types::ServerName<'static>> {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(rustls::pki_types::ServerName::IpAddress(ip.into()));
    }
    rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| anyhow::anyhow!("bad tls server name {host}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulseblade_core::{ActionRunStatus, Health, Observation, Resource, ResourceKind, RiskTier};
    use pulseblade_mcp::{query, ActionRoute};
    use std::sync::atomic::AtomicBool;
    use std::thread::JoinHandle;

    #[test]
    fn workload_routes_to_the_node_that_reported_it() {
        let store = Store::open_in_memory().unwrap();
        store
            .ingest_node(
                "web1",
                &NodeBatch {
                    changes: vec![pulseblade_core::Change {
                        seq: 1,
                        ts: chrono::Utc::now(),
                        resource_id: "workload:demo:ns:deployment:api".into(),
                        kind: pulseblade_core::ChangeKind::Appeared,
                        field: None,
                        before: None,
                        after: None,
                    }],
                    resources: vec![Resource::new(
                        "workload:demo:ns:deployment:api",
                        ResourceKind::Workload,
                        "api",
                    )
                    .health(Health::Failed)],
                    ..NodeBatch::default()
                },
            )
            .unwrap();
        let tokens = [("web1".to_string(), "t".to_string())];
        let node = route_node(&store, Some("workload:demo:ns:deployment:api"), &tokens).unwrap();
        assert_eq!(node, "web1");
        let pod = route_node(&store, Some("pod:demo:ns:api-0"), &tokens);
        assert!(pod.unwrap_err().contains("no enrolled node named demo"));
        assert_eq!(
            route_node(&store, Some("unit:web1:sshd.service"), &tokens).unwrap(),
            "web1"
        );
    }

    fn workload(ready: &str, health: Health) -> Resource {
        Resource::new(
            "workload:poc-apps:pulseblade:daemonset:pulseblade-prod",
            ResourceKind::Workload,
            "pulseblade-prod",
        )
        .health(health)
        .attr("kind", "daemonset")
        .attr("ready", ready)
    }

    fn apply_workload(store: &Store, ready: &str, health: Health) {
        store
            .apply(
                "k8s",
                chrono::Utc::now(),
                &Observation {
                    resources: vec![workload(ready, health)],
                    samples: vec![
                        pulseblade_core::Sample::new(
                            "workload:poc-apps:pulseblade:daemonset:pulseblade-prod",
                            "k8s.ready",
                            ready.split('/').next().unwrap().parse().unwrap(),
                        ),
                        pulseblade_core::Sample::new(
                            "workload:poc-apps:pulseblade:daemonset:pulseblade-prod",
                            "k8s.replicas",
                            9.0,
                        ),
                    ],
                    logs: vec![],
                },
            )
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rewound_journal_republishes_the_recovered_workload() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let hub = new_hub(
            store.clone(),
            vec![("poc-apps-2".into(), "token".into())],
            Duration::from_secs(2),
        );
        let stop_serve = shutdown.clone();
        let served = hub.clone();
        tokio::spawn(async move {
            serve(
                listener,
                served,
                vec!["127.0.0.1".into()],
                stop_serve,
                Arc::new(Registry::builtin()),
                None,
                PortalDefaults::default(),
            )
            .await
            .ok();
        });
        let client = HubClient::plain(format!("http://{addr}"), "token");

        let previous = Store::open_in_memory().unwrap();
        apply_workload(&previous, "9/9", Health::Ok);
        apply_workload(&previous, "8/9", Health::Degraded);
        client.push_store(&previous).unwrap();
        let id = "workload:poc-apps:pulseblade:daemonset:pulseblade-prod";
        let dipped = store.resource(id).unwrap().unwrap();
        assert_eq!(dipped.resource.health, Health::Degraded);
        assert_eq!(dipped.resource.attrs["ready"].as_str(), Some("8/9"));
        let stuck_cursor = client.cursor().unwrap().seq;
        assert!(stuck_cursor > 1, "{stuck_cursor}");

        // Rollout replaced the pod. The new emptyDir journal starts at seq 1
        // already healthy, which is below the hub cursor from the old process.
        let restarted = Store::open_in_memory().unwrap();
        apply_workload(&restarted, "9/9", Health::Ok);
        assert!(stuck_cursor > restarted.current_seq().unwrap());
        client.push_store(&restarted).unwrap();
        let healed = store.resource(id).unwrap().unwrap();
        assert_eq!(healed.resource.health, Health::Ok);
        assert_eq!(healed.resource.attrs["ready"].as_str(), Some("9/9"));
        assert_eq!(
            client.cursor().unwrap().seq,
            restarted.current_seq().unwrap()
        );

        apply_workload(&restarted, "7/9", Health::Degraded);
        client.push_store(&restarted).unwrap();
        let followed = store.resource(id).unwrap().unwrap();
        assert_eq!(followed.resource.health, Health::Degraded);
        assert_eq!(followed.resource.attrs["ready"].as_str(), Some("7/9"));
        assert_eq!(
            client.cursor().unwrap().seq,
            restarted.current_seq().unwrap()
        );
        shutdown.cancel();
    }

    #[test]
    fn hub_client_timeout_outlasts_the_long_poll() {
        // Nodes poll every 5s. The old deadline was poll_secs + 5 (10s), which
        // expired while a push sat in the hub SQLite queue.
        assert_eq!(hub_client_timeout(5), Duration::from_secs(30));
        for poll in [1_u64, 5, 15, 25, 30, 60] {
            let wait = Duration::from_secs(poll.clamp(1, 60).min(30));
            let timeout = hub_client_timeout(poll);
            assert!(
                timeout > wait,
                "timeout {timeout:?} must exceed long-poll {wait:?} for poll_secs={poll}"
            );
            assert!(timeout >= Duration::from_secs(30));
        }
    }

    struct Rec {
        calls: Mutex<Vec<String>>,
    }

    impl pulseblade_act::CommandRunner for Rec {
        fn run(&self, program: &str, args: &[String]) -> Result<String, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("{program} {}", args.join(" ")));
            Ok("ok".into())
        }
    }

    fn observe(store: &Store, resources: Vec<Resource>) {
        store
            .apply(
                "test",
                chrono::Utc::now(),
                &Observation {
                    resources,
                    samples: vec![pulseblade_core::Sample::new(
                        "host:web1",
                        "cpu.used_pct",
                        3.0,
                    )],
                    logs: vec![],
                },
            )
            .unwrap();
    }

    fn spawn_drainer(
        client: HubClient,
        store: Arc<Store>,
        reg: Arc<Registry>,
        stop: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = client.drain(&store, &reg);
                std::thread::sleep(Duration::from_millis(15));
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fleet_catches_up_and_nodes_keep_their_policy() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let hub = new_hub(
            store.clone(),
            vec![
                ("web1".into(), "token-web1".into()),
                ("web2".into(), "token-web2".into()),
            ],
            Duration::from_secs(3),
        );
        let stop_serve = shutdown.clone();
        let served = hub.clone();
        tokio::spawn(async move {
            serve(
                listener,
                served,
                vec!["127.0.0.1".into()],
                stop_serve,
                Arc::new(Registry::builtin()),
                None,
                PortalDefaults::default(),
            )
            .await
            .ok();
        });
        let base = format!("http://{addr}");
        let web1 = HubClient::plain(&base, "token-web1");
        let web2 = HubClient::plain(&base, "token-web2");

        let node1 = Arc::new(Store::open_in_memory().unwrap());
        observe(
            &node1,
            vec![
                Resource::new("host:web1", ResourceKind::Host, "web1")
                    .health(Health::Ok)
                    .label("role", "edge")
                    .label("cluster", "prod"),
                Resource::new(
                    "unit:web1:sshd.service",
                    ResourceKind::Service,
                    "sshd.service",
                )
                .parent("host:web1")
                .health(Health::Failed)
                .label("role", "edge")
                .label("criticality", "high"),
            ],
        );
        web1.push_store(&node1).unwrap();
        let mid = web1.cursor().unwrap().seq;
        observe(
            &node1,
            vec![
                Resource::new("host:web1", ResourceKind::Host, "web1")
                    .health(Health::Ok)
                    .label("role", "edge")
                    .label("cluster", "prod"),
                Resource::new(
                    "unit:web1:sshd.service",
                    ResourceKind::Service,
                    "sshd.service",
                )
                .parent("host:web1")
                .health(Health::Ok)
                .label("role", "edge")
                .label("criticality", "high"),
            ],
        );
        let gap = NodeBatch {
            changes: vec![pulseblade_core::Change {
                seq: mid + 5,
                ts: chrono::Utc::now(),
                resource_id: "host:web1".into(),
                kind: pulseblade_core::ChangeKind::Changed,
                field: Some("health".into()),
                before: None,
                after: None,
            }],
            ..NodeBatch::default()
        };
        let err = web1.push(&gap).unwrap_err().to_string();
        assert!(err.contains("409") || err.contains("gap"), "{err}");
        assert_eq!(web1.cursor().unwrap().seq, mid);
        web1.push_store(&node1).unwrap();
        assert_eq!(web1.cursor().unwrap().seq, node1.current_seq().unwrap());

        let node2 = Store::open_in_memory().unwrap();
        node2
            .apply(
                "test",
                chrono::Utc::now(),
                &Observation {
                    resources: vec![Resource::new("host:web2", ResourceKind::Host, "web2")
                        .health(Health::Degraded)
                        .label("role", "data")
                        .label("cluster", "prod")],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        web2.push_store(&node2).unwrap();

        let snap = query::snapshot(
            &store,
            query::SnapshotParams {
                detail: Some(query::Detail::Brief),
                ..Default::default()
            },
        )
        .unwrap();
        let ids: Vec<_> = snap
            .resources
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>();
        assert!(ids.contains(&"host:web1"), "{ids:?}");
        assert!(ids.contains(&"host:web2"), "{ids:?}");
        assert!(ids.iter().any(|id| id.contains("sshd")), "{ids:?}");
        let changes = query::changes(
            &store,
            query::ChangesParams {
                since: "seq:0".into(),
                kind: None,
                resource_prefix: None,
                limit: Some(100),
                compact: Some(false),
            },
        )
        .unwrap();
        assert!(
            changes.changes.iter().any(|c| {
                c.resource_id == "unit:web1:sshd.service" && c.field.as_deref() == Some("health")
            }),
            "offline changes did not catch up: {changes:?}"
        );
        let overview = query::fleet_overview(&store, query::FleetParams::default()).unwrap();
        assert_eq!(overview.hosts, 2);
        assert!(overview.unhealthy.iter().any(|r| r.id == "host:web2"));
        assert!(overview
            .groups
            .iter()
            .any(|g| g.host == "web1" && g.role.as_deref() == Some("edge")));

        let bad = ureq::get(&format!("{base}/hub/v1/cursor"))
            .set("Authorization", "Bearer nope")
            .call();
        assert!(matches!(bad, Err(ureq::Error::Status(401, _))));
        let evil = ureq::get(&format!("{base}/hub/v1/cursor"))
            .set("Authorization", "Bearer token-web1")
            .set("Host", "evil.example")
            .call();
        assert!(matches!(evil, Err(ureq::Error::Status(403, _))));

        let mut approve = pulseblade_act::ActionsConfig::default();
        approve.systemd_restart.tier = Some(RiskTier::Approve);
        let held = Arc::new(Rec {
            calls: Mutex::new(vec![]),
        });
        let reg1 = Arc::new(Registry::new(approve, held.clone()).unwrap());
        let mut auto = pulseblade_act::ActionsConfig::default();
        auto.systemd_restart.tier = Some(RiskTier::Auto);
        let ran = Arc::new(Rec {
            calls: Mutex::new(vec![]),
        });
        let reg2 = Arc::new(Registry::new(auto, ran.clone()).unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let d1 = spawn_drainer(web1.clone(), node1.clone(), reg1, stop.clone());
        let d2 = spawn_drainer(web2, Arc::new(node2), reg2, stop.clone());

        let waiting = hub
            .plan(pulseblade_act::PlanParams {
                action: "systemd.restart".into(),
                params: json!({"unit": "sshd.service"}),
                resource_id: Some("unit:web1:sshd.service".into()),
                idempotency_key: None,
                dry_run: false,
                finding_id: None,
            })
            .unwrap();
        assert_eq!(waiting.tier, RiskTier::Approve);
        let held_run = hub
            .execute(pulseblade_act::ExecuteParams {
                plan_id: waiting.plan_id.clone(),
            })
            .unwrap();
        assert_eq!(held_run.status, ActionRunStatus::AwaitingApproval);
        assert!(held.calls.lock().unwrap().is_empty());

        let go = hub
            .plan(pulseblade_act::PlanParams {
                action: "systemd.restart".into(),
                params: json!({"unit": "sshd.service"}),
                resource_id: Some("unit:web2:sshd.service".into()),
                idempotency_key: None,
                dry_run: false,
                finding_id: None,
            })
            .unwrap();
        assert_eq!(go.tier, RiskTier::Auto);
        let done = hub
            .execute(pulseblade_act::ExecuteParams {
                plan_id: go.plan_id,
            })
            .unwrap();
        assert_eq!(done.status, ActionRunStatus::Succeeded);
        assert!(
            ran.calls
                .lock()
                .unwrap()
                .iter()
                .any(|c| c.contains("systemctl")),
            "{:?}",
            ran.calls.lock().unwrap()
        );

        stop.store(true, Ordering::Relaxed);
        let _ = d1.join();
        let _ = d2.join();
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn portal_approve_runs_the_command_on_the_node() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let hub = new_hub(
            store.clone(),
            vec![("web1".into(), "token-web1".into())],
            Duration::from_secs(3),
        );
        let stop_serve = shutdown.clone();
        let served = hub.clone();
        tokio::spawn(async move {
            serve(
                listener,
                served,
                vec!["127.0.0.1".into()],
                stop_serve,
                Arc::new(Registry::builtin()),
                None,
                PortalDefaults::default(),
            )
            .await
            .ok();
        });
        let base = format!("http://{addr}");
        let web1 = HubClient::plain(&base, "token-web1");
        let node = Arc::new(Store::open_in_memory().unwrap());
        let mut approve = pulseblade_act::ActionsConfig::default();
        approve.systemd_restart.tier = Some(RiskTier::Approve);
        let held = Arc::new(Rec {
            calls: Mutex::new(vec![]),
        });
        let reg = Arc::new(Registry::new(approve, held.clone()).unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let drainer = spawn_drainer(web1, node.clone(), reg, stop.clone());

        let planned = hub
            .plan(pulseblade_act::PlanParams {
                action: "systemd.restart".into(),
                params: json!({"unit": "sshd.service"}),
                resource_id: Some("unit:web1:sshd.service".into()),
                idempotency_key: None,
                dry_run: false,
                finding_id: Some("fc13a29631a17b47555a0".into()),
            })
            .unwrap();
        let waiting = hub
            .execute(pulseblade_act::ExecuteParams {
                plan_id: planned.plan_id,
            })
            .unwrap();
        assert_eq!(waiting.status, ActionRunStatus::AwaitingApproval);
        assert!(
            held.calls.lock().unwrap().is_empty(),
            "approve tier must not run yet"
        );
        assert!(
            store.action(&waiting.id).unwrap().is_none(),
            "the hub action table does not hold the node run"
        );

        let listed: serde_json::Value = ureq::get(&format!("{base}/api/v1/actions"))
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert!(
            listed["runs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["id"] == waiting.id && r["status"] == "awaiting_approval"),
            "{listed}"
        );
        let status: serde_json::Value = ureq::get(&format!("{base}/api/v1/status"))
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(status["awaiting_actions"], 1);

        let approved: serde_json::Value =
            ureq::post(&format!("{base}/api/v1/actions/{}/approve", waiting.id))
                .call()
                .unwrap()
                .into_json()
                .unwrap();
        assert_eq!(approved["status"], "succeeded", "{approved}");
        let calls = held.calls.lock().unwrap().clone();
        assert!(
            calls.iter().any(|c| {
                c.contains("systemctl") && c.contains("restart") && c.contains("sshd.service")
            }),
            "node did not run the command: {calls:?}"
        );
        let on_node = node.action(&waiting.id).unwrap().unwrap();
        assert_eq!(on_node.status, ActionRunStatus::Succeeded);
        assert!(store.action(&waiting.id).unwrap().is_none());

        let after: serde_json::Value = ureq::get(&format!("{base}/api/v1/actions"))
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert!(
            after["runs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| { r["id"] == waiting.id && r["status"] == "succeeded" }),
            "{after}"
        );
        let status: serde_json::Value = ureq::get(&format!("{base}/api/v1/status"))
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(status["awaiting_actions"], 0);

        stop.store(true, Ordering::Relaxed);
        let _ = drainer.join();
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn portal_deny_cancels_without_running_the_command() {
        let store = Arc::new(Store::open_in_memory().unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let hub = new_hub(
            store.clone(),
            vec![("web1".into(), "token-web1".into())],
            Duration::from_secs(3),
        );
        let stop_serve = shutdown.clone();
        let served = hub.clone();
        tokio::spawn(async move {
            serve(
                listener,
                served,
                vec!["127.0.0.1".into()],
                stop_serve,
                Arc::new(Registry::builtin()),
                None,
                PortalDefaults::default(),
            )
            .await
            .ok();
        });
        let base = format!("http://{addr}");
        let web1 = HubClient::plain(&base, "token-web1");
        let node = Arc::new(Store::open_in_memory().unwrap());
        let mut approve = pulseblade_act::ActionsConfig::default();
        approve.systemd_restart.tier = Some(RiskTier::Approve);
        let held = Arc::new(Rec {
            calls: Mutex::new(vec![]),
        });
        let reg = Arc::new(Registry::new(approve, held.clone()).unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let drainer = spawn_drainer(web1, node.clone(), reg, stop.clone());

        let planned = hub
            .plan(pulseblade_act::PlanParams {
                action: "systemd.restart".into(),
                params: json!({"unit": "sshd.service"}),
                resource_id: Some("unit:web1:sshd.service".into()),
                idempotency_key: None,
                dry_run: false,
                finding_id: None,
            })
            .unwrap();
        let waiting = hub
            .execute(pulseblade_act::ExecuteParams {
                plan_id: planned.plan_id,
            })
            .unwrap();
        assert_eq!(waiting.status, ActionRunStatus::AwaitingApproval);

        let denied: serde_json::Value =
            ureq::post(&format!("{base}/api/v1/actions/{}/deny", waiting.id))
                .call()
                .unwrap()
                .into_json()
                .unwrap();
        assert_eq!(denied["status"], "rejected", "{denied}");
        let calls = held.calls.lock().unwrap().clone();
        assert!(
            calls.is_empty(),
            "deny must not start the command: {calls:?}"
        );
        let on_node = node.action(&waiting.id).unwrap().unwrap();
        assert_eq!(on_node.status, ActionRunStatus::Rejected);
        assert!(store.action(&waiting.id).unwrap().is_none());

        stop.store(true, Ordering::Relaxed);
        let _ = drainer.join();
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mtls_requires_a_client_cert_and_the_token() {
        let dir = std::env::temp_dir().join(format!("pulseblade-mtls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let ca = ca_params.self_signed(&ca_key).unwrap();

        let server_key = rcgen::KeyPair::generate().unwrap();
        let mut server_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        server_params.subject_alt_names = vec![rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        ))];
        let server = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();

        let client_key = rcgen::KeyPair::generate().unwrap();
        let client_params = rcgen::CertificateParams::new(vec!["node-web1".into()]).unwrap();
        let client = client_params.signed_by(&client_key, &ca, &ca_key).unwrap();

        let ca_path = dir.join("ca.pem");
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        let client_cert = dir.join("client.pem");
        let client_key_path = dir.join("client.key");
        std::fs::write(&ca_path, ca.pem()).unwrap();
        std::fs::write(&cert_path, server.pem()).unwrap();
        std::fs::write(&key_path, server_key.serialize_pem()).unwrap();
        std::fs::write(&client_cert, client.pem()).unwrap();
        std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();

        let tls = server_config(&crate::config::HubTls {
            cert: cert_path.display().to_string(),
            key: key_path.display().to_string(),
            client_ca: ca_path.display().to_string(),
        })
        .unwrap();
        let store = Arc::new(Store::open_in_memory().unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let hub = new_hub(
            store,
            vec![("web1".into(), "token-web1".into())],
            Duration::from_secs(2),
        );
        tokio::spawn(async move {
            serve(
                listener,
                hub,
                vec!["127.0.0.1".into()],
                stop,
                Arc::new(Registry::builtin()),
                Some(tls),
                PortalDefaults::default(),
            )
            .await
            .ok();
        });

        let base = format!("https://127.0.0.1:{}", addr.port());
        install_crypto();
        let mut roots = rustls::RootCertStore::empty();
        for c in load_certs(&ca_path.display().to_string()).unwrap() {
            roots.add(c).unwrap();
        }
        let anon = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let denied = tls_exchange(
            &anon,
            &format!("{base}/hub/v1/cursor"),
            "token-web1",
            "GET",
            None,
        );
        assert!(
            denied.is_err(),
            "a client without a certificate must be rejected"
        );

        let authed = HubClient {
            base: base.clone(),
            token: "token-web1".into(),
            agent: ureq::AgentBuilder::new().build(),
            interval_secs: 15,
            tls: Some(
                client_config(
                    &ca_path.display().to_string(),
                    &client_cert.display().to_string(),
                    &client_key_path.display().to_string(),
                )
                .unwrap(),
            ),
        };
        assert_eq!(authed.cursor().unwrap().seq, 0);
        let bad = HubClient {
            token: "nope".into(),
            ..authed
        };
        let err = bad.cursor().unwrap_err().to_string();
        assert!(err.contains("401"), "{err}");
        shutdown.cancel();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn https_bearer_does_not_require_a_client_cert() {
        let dir = std::env::temp_dir().join(format!("pulseblade-https-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let ca = ca_params.self_signed(&ca_key).unwrap();

        let server_key = rcgen::KeyPair::generate().unwrap();
        let mut server_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        server_params.subject_alt_names = vec![rcgen::SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        ))];
        let server = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();

        let ca_path = dir.join("ca.pem");
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&ca_path, ca.pem()).unwrap();
        std::fs::write(&cert_path, server.pem()).unwrap();
        std::fs::write(&key_path, server_key.serialize_pem()).unwrap();

        install_crypto();
        let tls = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    load_certs(&cert_path.display().to_string()).unwrap(),
                    load_key(&key_path.display().to_string()).unwrap(),
                )
                .unwrap(),
        );
        let store = Arc::new(Store::open_in_memory().unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let hub = new_hub(
            store,
            vec![("web1".into(), "token-web1".into())],
            Duration::from_secs(2),
        );
        tokio::spawn(async move {
            serve(
                listener,
                hub,
                vec!["127.0.0.1".into()],
                stop,
                Arc::new(Registry::builtin()),
                Some(tls),
                PortalDefaults::default(),
            )
            .await
            .ok();
        });

        let mut roots = rustls::RootCertStore::empty();
        for c in load_certs(&ca_path.display().to_string()).unwrap() {
            roots.add(c).unwrap();
        }
        let agent = ureq::AgentBuilder::new()
            .tls_config(Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ))
            .build();
        let authed = HubClient {
            base: format!("https://127.0.0.1:{}", addr.port()),
            token: "token-web1".into(),
            agent,
            tls: None,
            interval_secs: 15,
        };
        assert_eq!(authed.cursor().unwrap().seq, 0);
        let err = HubClient {
            token: "nope".into(),
            ..authed
        }
        .cursor()
        .unwrap_err()
        .to_string();
        assert!(err.contains("401"), "{err}");
        shutdown.cancel();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_serves_cached_snapshot_then_accepts_push() {
        let dir = std::env::temp_dir().join(format!(
            "pb-recover-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_millis()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cache = dir.join("snapshot.json");
        let store = Arc::new(Store::open_in_memory().unwrap());
        store
            .apply(
                "host",
                chrono::Utc::now(),
                &Observation {
                    resources: vec![Resource::new("host:cached", ResourceKind::Host, "cached")
                        .health(Health::Ok)
                        .label("host", "cached")],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        let summary = query::snapshot(
            &store,
            query::SnapshotParams {
                detail: Some(query::Detail::Summary),
                ..Default::default()
            },
        )
        .unwrap();
        let seq = summary.as_of_seq;
        assert!(seq > 0, "{seq}");
        let journal = pulseblade_mcp::journal::Journal::recover(&cache);
        journal.remember(&summary);
        let cached = std::fs::read(&cache).unwrap();
        assert!(!journal.is_up());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let hub = new_hub_journal(
            journal.clone(),
            vec![("web1".into(), "token".into())],
            Duration::from_secs(2),
        );
        let stop = shutdown.clone();
        tokio::spawn(async move {
            serve(
                listener,
                hub,
                vec!["127.0.0.1".into()],
                stop,
                Arc::new(Registry::builtin()),
                None,
                PortalDefaults::default(),
            )
            .await
            .ok();
        });
        let base = format!("http://{addr}");
        let snapshot = format!("{base}/api/v1/snapshot");
        let mut body = None;
        for _ in 0..50 {
            if let Ok(resp) = ureq::get(&snapshot).call() {
                body = Some(resp.into_json::<serde_json::Value>().unwrap());
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let body = body.expect("hub did not bind");
        assert_eq!(body["stale"], true);
        assert_eq!(body["as_of_seq"], seq);
        let status: serde_json::Value = ureq::get(&format!("{base}/api/v1/status"))
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(status["stale"], true);
        assert_eq!(status["as_of_seq"], seq);
        let findings: serde_json::Value = ureq::get(&format!("{base}/api/v1/findings"))
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(findings["stale"], true);
        let metrics = ureq::get(&format!("{base}/metrics"))
            .call()
            .unwrap()
            .into_string()
            .unwrap();
        let metrics: serde_json::Value = serde_json::from_str(&metrics).unwrap();
        assert_eq!(metrics["stale"], true);
        assert_eq!(metrics["as_of_seq"], seq);

        let node = Store::open_in_memory().unwrap();
        node.apply(
            "host",
            chrono::Utc::now(),
            &Observation {
                resources: vec![Resource::new("host:web1", ResourceKind::Host, "web1")
                    .health(Health::Degraded)
                    .label("host", "web1")],
                samples: vec![],
                logs: vec![],
            },
        )
        .unwrap();
        let client = HubClient::plain(&base, "token");
        let err = client.push_store(&node).unwrap_err().to_string();
        assert!(err.contains("503"), "{err}");
        assert_eq!(store.current_seq().unwrap(), seq);
        assert!(store.resource("host:web1").unwrap().is_none());
        assert_eq!(std::fs::read(&cache).unwrap(), cached);

        let pushed = batch_since(
            &node,
            NodeCursor {
                seq: 0,
                sample_ms: 0,
                bucket_ms: 0,
                event_id: 0,
            },
        )
        .unwrap();
        let rejected = ureq::post(&format!("{base}/hub/v1/push"))
            .set("Authorization", "Bearer token")
            .send_json(serde_json::to_value(&pushed).unwrap());
        match rejected {
            Err(ureq::Error::Status(503, _)) => {}
            other => panic!("push during recovery was not 503: {other:?}"),
        }
        assert_eq!(store.current_seq().unwrap(), seq);

        journal.install(store.clone());
        client.push_store(&node).unwrap();
        assert!(store.current_seq().unwrap() > seq);
        let healed = store.resource("host:web1").unwrap().unwrap();
        assert_eq!(healed.resource.health, Health::Degraded);
        let live: serde_json::Value = ureq::get(&snapshot).call().unwrap().into_json().unwrap();
        assert!(live.get("stale").is_none(), "{live}");
        assert!(live["as_of_seq"].as_i64().unwrap() > seq);

        shutdown.cancel();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
