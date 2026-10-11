mod config;
mod hub;
mod node;
mod status;
mod webhook;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use pulseblade_collect::default_collectors;
use pulseblade_core::{Aggregation, ResourceKind};
use pulseblade_mcp::query;
use pulseblade_store::Store;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::config::{default_db_path, Config};
use crate::node::{collect_once, detect_pass, spawn_collector, CollectorLock};

/// Agent-first infrastructure monitor: MCP-native telemetry, change checkpoints,
/// and gated remediation.
#[derive(Parser)]
#[command(name = "pulseblade", version, about)]
struct Cli {
    /// SQLite database path [default: $XDG_STATE_HOME/pulseblade/pulseblade.db]
    #[arg(long, global = true, env = "PULSEBLADE_DB")]
    db: Option<PathBuf>,

    /// Config file [default: $XDG_CONFIG_HOME/pulseblade/pulseblade.toml]
    #[arg(long, global = true, env = "PULSEBLADE_CONFIG")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run this host's node: collectors plus MCP over streamable HTTP.
    #[command(alias = "agent")]
    Node {
        /// Listen address [default: from config, 127.0.0.1:7171]
        #[arg(long)]
        listen: Option<SocketAddr>,
        /// Serve an existing database without collecting: HTTP, MCP, and dashboard,
        /// no collectors, and no collector lock (offline inspection, demo data).
        #[arg(long)]
        no_collect: bool,
    },
    /// Serve MCP over stdio; collects in-process unless a node owns the database.
    /// With `--dashboard` or `--listen`, also serves the HTTP dashboard and API.
    Mcp {
        /// Also serve the HTTP dashboard, JSON API, Prometheus metrics, and `/mcp`.
        #[arg(long)]
        dashboard: bool,
        /// HTTP listen address; implies `--dashboard` [default: from config, 127.0.0.1:7171]
        #[arg(long)]
        listen: Option<SocketAddr>,
    },
    /// Control node: aggregate many nodes into fleet state (M6).
    Hub,
    /// Investigate findings with an OpenAI-compatible model. Talks to a node or a hub over MCP.
    Pilot {
        /// MCP endpoint [default: pilot.mcp_url, else http://<listen>/mcp]
        #[arg(long)]
        mcp_url: Option<String>,
        /// Poll once, then exit. A quiet poll does not call the model.
        #[arg(long)]
        once: bool,
    },
    /// Query and manage the local store from the shell.
    Ctl {
        #[command(subcommand)]
        command: Ctl,
    },
}

#[derive(Subcommand)]
enum Ctl {
    /// Human-readable summary: self-health, counts, unhealthy resources, recent changes.
    Status {
        /// How many recent changes to show.
        #[arg(long, default_value_t = 10)]
        changes: usize,
    },
    /// Current state as JSON, unhealthy first.
    Snapshot {
        /// summary, brief, or full [default: summary without filters, brief with]
        #[arg(long, value_parser = parse_detail)]
        detail: Option<query::Detail>,
        #[arg(long)]
        kind: Option<ResourceKind>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long)]
        unhealthy: bool,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Everything about one resource.
    Explain { id: String },
    /// Downsampled history of one metric.
    Metrics {
        id: String,
        metric: String,
        #[arg(long)]
        range: Option<String>,
        #[arg(long)]
        step: Option<String>,
        #[arg(long, value_parser = parse_agg)]
        agg: Option<Aggregation>,
    },
    /// Name the current journal position.
    Checkpoint { name: String },
    /// Changes since a checkpoint, seq:<n>, timestamp, or duration.
    Changes {
        #[arg(long)]
        since: String,
        #[arg(long)]
        kind: Option<ResourceKind>,
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        /// Include a timestamp on every change.
        #[arg(long)]
        timestamps: bool,
    },
    /// JSONL dump of resources (and changes with --since).
    Export {
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        kind: Option<ResourceKind>,
        #[arg(long)]
        max_lines: Option<usize>,
    },
    /// Run one collection pass now (fails if a node is already collecting).
    Collect,
    /// Open findings by default.
    Findings {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        /// Include causal context and error lines.
        #[arg(long)]
        detail: bool,
    },
    /// Acknowledge a finding, optionally recording a note.
    Ack {
        id: String,
        #[arg(long)]
        note: Option<String>,
    },
    /// Attach a note to a resource.
    Note { resource: String, text: String },
    /// Recent action runs, newest first.
    Actions,
    /// Approve an action that is waiting for a human.
    Approve { run_id: String },
    /// Recent pilot investigations, newest first.
    Runs,
}

fn parse_agg(s: &str) -> Result<Aggregation, String> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|_| "expected avg, min, max, or last".to_string())
}

fn parse_detail(s: &str) -> Result<query::Detail, String> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|_| "expected summary, brief, or full".to_string())
}

fn open_store(path: &Path) -> anyhow::Result<Arc<Store>> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    Ok(Arc::new(
        Store::open(path).with_context(|| format!("opening {}", path.display()))?,
    ))
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,rmcp=warn".into()),
        )
        // stdout belongs to the MCP stdio transport.
        .with_writer(std::io::stderr)
        .init();
}

fn print_json<T: Serialize>(v: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

/// Hosts accepted by the HTTP server: loopback, the listen address, and config extras.
fn http_allowed_hosts(config: &Config, addr: SocketAddr) -> Vec<String> {
    let mut allowed = vec!["localhost".into(), "127.0.0.1".into(), "::1".into()];
    allowed.push(addr.ip().to_string());
    allowed.extend(config.allowed_hosts.iter().cloned());
    allowed
}

/// Listen address for the MCP dashboard, if it should run.
///
/// `--listen` or `--dashboard` enables it; otherwise `config.dashboard` does.
/// `--listen` overrides `config.listen`.
fn mcp_dashboard_addr(
    dashboard: bool,
    listen: Option<SocketAddr>,
    config: &Config,
) -> anyhow::Result<Option<SocketAddr>> {
    if listen.is_none() && !dashboard && !config.dashboard {
        return Ok(None);
    }
    Ok(Some(match listen {
        Some(addr) => addr,
        None => config
            .listen
            .parse()
            .with_context(|| format!("invalid listen address `{}`", config.listen))?,
    }))
}

fn is_addr_in_use(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::AddrInUse)
    })
}

/// Serve the HTTP dashboard until `shutdown`. A busy port is logged and ignored so
/// stdio MCP keeps running when several `pulseblade mcp` processes start together.
fn hub_tokens(config: &Config) -> anyhow::Result<Vec<(String, String)>> {
    if config.hub.nodes.is_empty() {
        anyhow::bail!("hub needs at least one [[hub.nodes]] entry with id and token_env");
    }
    config
        .hub
        .nodes
        .iter()
        .map(|node| {
            let token = std::env::var(&node.token_env).with_context(|| {
                format!("node {} token_env {} is unset", node.id, node.token_env)
            })?;
            anyhow::ensure!(!token.is_empty(), "node {} token is empty", node.id);
            Ok((node.id.clone(), token))
        })
        .collect()
}

fn spawn_upstream(
    store: Arc<Store>,
    actions: Arc<pulseblade_act::Registry>,
    config: &Config,
    shutdown: CancellationToken,
) -> Option<std::thread::JoinHandle<()>> {
    let url = config.upstream.url.trim();
    if url.is_empty() {
        return None;
    }
    let token = match std::env::var(&config.upstream.token_env) {
        Ok(t) if !t.is_empty() => t,
        _ => {
            tracing::warn!(
                env = %config.upstream.token_env,
                "upstream url is set but the token env var is empty; not connecting"
            );
            return None;
        }
    };
    let tls = if config.upstream.cert.is_empty() && config.upstream.key.is_empty() {
        None
    } else {
        match hub::client_config(
            &config.upstream.ca,
            &config.upstream.cert,
            &config.upstream.key,
        ) {
            Ok(cfg) => Some(cfg),
            Err(e) => {
                tracing::warn!(error = %e, "upstream tls config failed; not connecting");
                return None;
            }
        }
    };
    let poll = config.upstream.poll_secs;
    let base = url.to_string();
    Some(std::thread::spawn(move || {
        hub::run_upstream(store, actions, base, token, tls, poll, shutdown);
    }))
}

fn action_registry(config: &Config) -> anyhow::Result<Arc<pulseblade_act::Registry>> {
    let mut reg = pulseblade_act::Registry::new(
        config.actions.clone(),
        Arc::new(pulseblade_act::SystemRunner),
    )
    .map_err(|e| anyhow::anyhow!(e))?;
    reg.set_proxmox(config.proxmox.endpoint());
    Ok(Arc::new(reg))
}

async fn serve_mcp_dashboard(
    store: Arc<Store>,
    addr: SocketAddr,
    allowed: Vec<String>,
    shutdown: CancellationToken,
    actions: Arc<pulseblade_act::Registry>,
) {
    match pulseblade_mcp::serve_http(store, addr, allowed, shutdown, actions).await {
        Ok(()) => {}
        Err(e) if is_addr_in_use(&e) => {
            tracing::warn!(
                %addr,
                "listen address already in use; serving stdio MCP without the dashboard"
            );
        }
        Err(e) => {
            tracing::error!(
                %addr,
                error = %e,
                "HTTP server failed; serving stdio MCP without the dashboard"
            );
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing();
    let config = Config::load(cli.config.as_deref())?;
    let db = cli.db.clone().unwrap_or_else(default_db_path);

    match cli.command {
        Command::Node { listen, no_collect } => {
            if no_collect {
                anyhow::ensure!(db.exists(), "database {} does not exist", db.display());
            }
            let store = open_store(&db)?;
            let lock = if no_collect {
                tracing::info!(db = %db.display(), "serving without collecting");
                None
            } else {
                Some(CollectorLock::try_acquire(&db)?.with_context(|| {
                    format!(
                        "another pulseblade process is already collecting into {}",
                        db.display()
                    )
                })?)
            };
            let addr = match listen {
                Some(a) => a,
                None => config
                    .listen
                    .parse()
                    .with_context(|| format!("invalid listen address `{}`", config.listen))?,
            };
            let shutdown = CancellationToken::new();
            let collector =
                lock.map(|lock| spawn_collector(store.clone(), &config, lock, shutdown.clone()));
            let ctrl_c = shutdown.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutting down");
                ctrl_c.cancel();
            });
            let allowed = http_allowed_hosts(&config, addr);
            let actions = action_registry(&config)?;
            let upstream =
                spawn_upstream(store.clone(), actions.clone(), &config, shutdown.clone());
            let served =
                pulseblade_mcp::serve_http(store, addr, allowed, shutdown.clone(), actions).await;
            shutdown.cancel();
            if let Some(c) = collector {
                let _ = tokio::task::spawn_blocking(move || c.join()).await;
            }
            if let Some(u) = upstream {
                let _ = tokio::task::spawn_blocking(move || u.join()).await;
            }
            served
        }
        Command::Mcp { dashboard, listen } => {
            let store = open_store(&db)?;
            let shutdown = CancellationToken::new();
            let dashboard_addr = mcp_dashboard_addr(dashboard, listen, &config)?;
            let collector = match CollectorLock::try_acquire(&db)? {
                Some(lock) => Some(spawn_collector(
                    store.clone(),
                    &config,
                    lock,
                    shutdown.clone(),
                )),
                None => {
                    tracing::info!(
                        "node already collecting; serving its database without collecting"
                    );
                    None
                }
            };
            let actions = action_registry(&config)?;
            let http_task = if let Some(addr) = dashboard_addr {
                let allowed = http_allowed_hosts(&config, addr);
                let http_store = store.clone();
                let http_actions = actions.clone();
                let http_shutdown = shutdown.clone();
                let ctrl_c = shutdown.clone();
                tokio::spawn(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    tracing::info!("shutting down");
                    ctrl_c.cancel();
                });
                Some(tokio::spawn(async move {
                    serve_mcp_dashboard(http_store, addr, allowed, http_shutdown, http_actions)
                        .await;
                }))
            } else {
                None
            };
            let served = if http_task.is_some() {
                tokio::select! {
                    result = pulseblade_mcp::serve_stdio(store, actions) => result,
                    () = shutdown.cancelled() => Ok(()),
                }
            } else {
                pulseblade_mcp::serve_stdio(store, actions).await
            };
            shutdown.cancel();
            if let Some(task) = http_task {
                let _ = task.await;
            }
            if let Some(c) = collector {
                let _ = tokio::task::spawn_blocking(move || c.join()).await;
            }
            served
        }
        Command::Hub => {
            let store = open_store(&db)?;
            let tokens = hub_tokens(&config)?;
            let addr: SocketAddr =
                config.hub.listen.parse().with_context(|| {
                    format!("invalid hub listen address `{}`", config.hub.listen)
                })?;
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding {}", config.hub.listen))?;
            let tls = if config.hub.tls.enabled() {
                Some(hub::server_config(&config.hub.tls)?)
            } else {
                None
            };
            let shutdown = CancellationToken::new();
            let ctrl_c = shutdown.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutting down");
                ctrl_c.cancel();
            });
            let allowed = http_allowed_hosts(&config, listener.local_addr()?);
            let actions = action_registry(&config)?;
            let inner = hub::new_hub(
                store.clone(),
                tokens,
                std::time::Duration::from_secs(config.hub.action_wait_secs.max(1)),
            );
            hub::serve(listener, inner, allowed, shutdown, actions, tls).await
        }
        Command::Pilot { mcp_url, once } => {
            let url = pilot_mcp_url(&config, mcp_url);
            let store = open_store(&db)?;
            let pilot_cfg = config.pilot.clone();
            if once {
                run_pilot(store, pilot_cfg, url, None)
            } else {
                let shutdown = CancellationToken::new();
                let ctrl = shutdown.clone();
                tokio::spawn(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    ctrl.cancel();
                });
                tokio::task::spawn_blocking(move || {
                    run_pilot(store, pilot_cfg, url, Some(shutdown))
                })
                .await?
            }
        }
        Command::Ctl { command } => {
            let store = open_store(&db)?;
            run_ctl(&store, &db, &config, command)
        }
    }
}

fn run_ctl(store: &Store, db: &Path, config: &Config, command: Ctl) -> anyhow::Result<()> {
    match command {
        Ctl::Status { changes } => {
            print!("{}", status::render(store, changes)?);
            Ok(())
        }
        Ctl::Snapshot {
            detail,
            kind,
            query: q,
            unhealthy,
            limit,
        } => print_json(&query::snapshot(
            store,
            query::SnapshotParams {
                detail,
                if_changed_since: None,
                kind,
                query: q,
                labels: None,
                unhealthy_only: unhealthy.then_some(true),
                limit,
            },
        )?),
        Ctl::Explain { id } => print_json(&query::explain(
            store,
            query::ExplainParams {
                id,
                changes_limit: None,
            },
        )?),
        Ctl::Metrics {
            id,
            metric,
            range,
            step,
            agg,
        } => print_json(&query::metrics(
            store,
            query::MetricsParams {
                id,
                metric,
                range,
                step,
                agg,
            },
        )?),
        Ctl::Checkpoint { name } => {
            print_json(&query::checkpoint(store, query::CheckpointParams { name })?)
        }
        Ctl::Changes {
            since,
            kind,
            prefix,
            limit,
            timestamps,
        } => print_json(&query::changes(
            store,
            query::ChangesParams {
                since,
                kind,
                resource_prefix: prefix,
                limit,
                compact: Some(!timestamps),
            },
        )?),
        Ctl::Export {
            since,
            kind,
            max_lines,
        } => {
            println!(
                "{}",
                query::export(
                    store,
                    query::ExportParams {
                        since,
                        kind,
                        max_lines,
                    },
                )?
            );
            Ok(())
        }
        Ctl::Collect => {
            let _lock = CollectorLock::try_acquire(db)?
                .context("a node is already collecting into this database")?;
            let opts = config.collector_opts();
            let mut collectors = default_collectors(&opts);
            let n = collect_once(store, &mut collectors, &config.labels, None);
            detect_pass(store, &config.detect);
            print_json(&serde_json::json!({
                "changes": n,
                "as_of_seq": store.current_seq()?,
            }))
        }
        Ctl::Findings {
            status,
            limit,
            detail,
        } => {
            let status = status
                .as_deref()
                .map(|s| s.parse().map_err(|e: String| anyhow::anyhow!(e)))
                .transpose()?;
            print_json(&query::findings(
                store,
                query::FindingsParams {
                    status,
                    severity: None,
                    limit,
                    detail: Some(detail),
                },
            )?)
        }
        Ctl::Ack { id, note } => {
            print_json(&query::ack_finding(store, query::AckParams { id, note })?)
        }
        Ctl::Note { resource, text } => print_json(&query::memory_note(
            store,
            query::NoteParams {
                resource,
                text,
                author: Some("ctl".into()),
            },
        )?),
        Ctl::Actions => print_json(&store.recent_actions(None, 20)?),
        Ctl::Approve { run_id } => {
            let reg = action_registry(config)?;
            print_json(&reg.approve(store, &run_id)?)
        }
        Ctl::Runs => print_json(&store.recent_pilot_runs(20)?),
    }
}

fn pilot_mcp_url(config: &Config, flag: Option<String>) -> String {
    if let Some(url) = flag.filter(|s| !s.is_empty()) {
        return url;
    }
    if !config.pilot.mcp_url.is_empty() {
        return config.pilot.mcp_url.clone();
    }
    format!("http://{}/mcp", config.listen)
}

fn run_pilot(
    store: Arc<Store>,
    config: pulseblade_pilot::PilotConfig,
    mcp_url: String,
    shutdown: Option<CancellationToken>,
) -> anyhow::Result<()> {
    let mut session = pulseblade_pilot::Session::new(config.clone(), std::time::Instant::now());
    let mut chat = pulseblade_pilot::HttpChat;
    let poll = std::time::Duration::from_secs(config.poll_secs.max(1));
    loop {
        if shutdown.as_ref().is_some_and(|s| s.is_cancelled()) {
            break;
        }
        let mut tools = pulseblade_pilot::McpClient::new(&mcp_url);
        match session.tick(&mut tools, &mut chat, std::time::Instant::now()) {
            Ok(None) => {
                if shutdown.is_none() {
                    println!("{{\"investigated\":false}}");
                    break;
                }
            }
            Ok(Some(run)) => {
                let id = store.insert_pilot_run(
                    &pulseblade_store::NewPilotRun {
                        trigger: run.trigger.clone(),
                        tier: run.tier.clone(),
                        endpoint: run.endpoint.clone(),
                        tool_calls: run.tool_calls,
                        tokens: run.tokens,
                        conclusion: run.conclusion.clone(),
                        proposed: run.proposed.clone(),
                        status: run.status.clone(),
                        detail: run.detail.clone(),
                    },
                    chrono::Utc::now(),
                )?;
                print_json(&serde_json::json!({
                    "id": id,
                    "status": run.status,
                    "trigger": run.trigger,
                    "tier": run.tier,
                    "endpoint": run.endpoint,
                    "tool_calls": run.tool_calls,
                    "tokens": run.tokens,
                    "conclusion": run.conclusion,
                    "proposed": run.proposed,
                }))?;
                if shutdown.is_none() {
                    break;
                }
            }
            Err(e) => {
                if shutdown.is_none() {
                    anyhow::bail!(e);
                }
                tracing::warn!(error = %e, "pilot poll failed");
            }
        }
        let Some(shutdown) = shutdown.as_ref() else {
            break;
        };
        let deadline = std::time::Instant::now() + poll;
        while !shutdown.is_cancelled() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Command {
        Cli::try_parse_from(args).unwrap().command
    }

    #[test]
    fn mcp_flags_parse() {
        match parse(&["pulseblade", "mcp"]) {
            Command::Mcp { dashboard, listen } => {
                assert!(!dashboard);
                assert!(listen.is_none());
            }
            _ => panic!("expected mcp"),
        }
        match parse(&["pulseblade", "mcp", "--dashboard"]) {
            Command::Mcp { dashboard, listen } => {
                assert!(dashboard);
                assert!(listen.is_none());
            }
            _ => panic!("expected mcp"),
        }
        match parse(&["pulseblade", "mcp", "--listen", "127.0.0.1:7391"]) {
            Command::Mcp { dashboard, listen } => {
                assert!(!dashboard);
                assert_eq!(listen, Some("127.0.0.1:7391".parse().unwrap()));
            }
            _ => panic!("expected mcp"),
        }
    }

    #[test]
    fn mcp_dashboard_addr_follows_flag_then_config() {
        let mut config = Config::default();
        assert!(mcp_dashboard_addr(false, None, &config).unwrap().is_none());

        config.dashboard = true;
        assert_eq!(
            mcp_dashboard_addr(false, None, &config).unwrap(),
            Some("127.0.0.1:7171".parse().unwrap())
        );

        config.dashboard = false;
        config.listen = "127.0.0.1:7172".into();
        assert_eq!(
            mcp_dashboard_addr(true, None, &config).unwrap(),
            Some("127.0.0.1:7172".parse().unwrap())
        );

        let flag: SocketAddr = "127.0.0.1:7391".parse().unwrap();
        assert_eq!(
            mcp_dashboard_addr(false, Some(flag), &config).unwrap(),
            Some(flag)
        );
        config.dashboard = true;
        assert_eq!(
            mcp_dashboard_addr(true, Some(flag), &config).unwrap(),
            Some(flag)
        );

        config.listen = "not-an-address".into();
        assert!(mcp_dashboard_addr(false, None, &config).is_err());
        assert_eq!(
            mcp_dashboard_addr(false, Some(flag), &config).unwrap(),
            Some(flag)
        );
    }

    #[test]
    fn addr_in_use_is_recognized() {
        let err = anyhow::Error::from(std::io::Error::new(std::io::ErrorKind::AddrInUse, "in use"));
        assert!(is_addr_in_use(&err));
        assert!(!is_addr_in_use(&anyhow::Error::msg("nope")));
    }
}
