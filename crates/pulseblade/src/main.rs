mod config;
mod node;
mod status;

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
use crate::node::{collect_once, spawn_collector, CollectorLock};

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
    Mcp,
    /// Control node: aggregate many nodes into fleet state (M6).
    Hub,
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
            let mut allowed = vec!["localhost".into(), "127.0.0.1".into(), "::1".into()];
            allowed.push(addr.ip().to_string());
            allowed.extend(config.allowed_hosts.iter().cloned());
            let served = pulseblade_mcp::serve_http(store, addr, allowed, shutdown.clone()).await;
            shutdown.cancel();
            if let Some(c) = collector {
                let _ = tokio::task::spawn_blocking(move || c.join()).await;
            }
            served
        }
        Command::Mcp => {
            let store = open_store(&db)?;
            let shutdown = CancellationToken::new();
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
            let served = pulseblade_mcp::serve_stdio(store).await;
            shutdown.cancel();
            if let Some(c) = collector {
                let _ = tokio::task::spawn_blocking(move || c.join()).await;
            }
            served
        }
        Command::Hub => {
            anyhow::bail!(
                "hub mode is planned for M6: https://github.com/DataKnifeAI/pulseblade/issues/4"
            )
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
            let mut collectors = default_collectors();
            let n = collect_once(store, &mut collectors, &config.labels, None);
            print_json(&serde_json::json!({
                "changes": n,
                "as_of_seq": store.current_seq()?,
            }))
        }
    }
}
