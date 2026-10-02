use std::fs::{File, TryLockError};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use pulseblade_collect::{default_collectors, Collector};
use pulseblade_core::{ChangeKind, CollectorRun, LabelRule};
use pulseblade_store::Store;
use tokio_util::sync::CancellationToken;

use crate::config::Config;

const PRUNE_EVERY: Duration = Duration::from_secs(3600);

/// Exclusive right to collect into one database; released on drop.
pub struct CollectorLock {
    _file: File,
}

impl CollectorLock {
    /// `Ok(None)` when another process already collects into this database.
    pub fn try_acquire(db: &Path) -> anyhow::Result<Option<Self>> {
        let path = db.with_extension("db.lock");
        let file = File::create(&path).with_context(|| format!("creating {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(e)) => Err(e).context("locking collector lock file"),
        }
    }
}

/// Run one pass of every collector and record the results, including a
/// [`CollectorRun`] per collector. `interval_secs` is `None` for one-off passes.
pub fn collect_once(
    store: &Store,
    collectors: &mut [Box<dyn Collector>],
    labels: &[LabelRule],
    interval_secs: Option<u64>,
) -> usize {
    let mut total = 0;
    for c in collectors.iter_mut() {
        let started = Instant::now();
        let ts = Utc::now();
        let mut run = CollectorRun {
            source: c.name().to_string(),
            ts,
            duration_ms: 0,
            ok: false,
            error: None,
            resource_count: 0,
            sample_count: 0,
            change_count: 0,
            interval_secs,
        };
        let applied = c
            .collect()
            .map_err(|e| {
                tracing::warn!(collector = c.name(), error = %e, "collection failed");
                format!("collect: {e}")
            })
            .and_then(|mut obs| {
                for r in &mut obs.resources {
                    for rule in labels {
                        rule.apply(r);
                    }
                }
                run.resource_count = obs.resources.len();
                run.sample_count = obs.samples.len();
                store.apply(c.name(), ts, &obs).map_err(|e| {
                    tracing::error!(collector = c.name(), error = %e, "store write failed");
                    format!("store: {e}")
                })
            });
        run.duration_ms = started.elapsed().as_millis() as u64;
        match applied {
            Ok(changes) => {
                run.ok = true;
                run.change_count = changes.len();
                total += changes.len();
                for ch in changes.iter().filter(|ch| {
                    ch.kind == ChangeKind::Disappeared || ch.field.as_deref() == Some("health")
                }) {
                    tracing::info!(
                        seq = ch.seq,
                        resource = %ch.resource_id,
                        kind = ch.kind.as_str(),
                        before = ?ch.before,
                        after = ?ch.after,
                        "change"
                    );
                }
            }
            Err(e) => run.error = Some(e),
        }
        if let Err(e) = store.record_run(&run) {
            tracing::warn!(collector = c.name(), error = %e, "recording collector run failed");
        }
    }
    total
}

/// Collect on a dedicated thread every `interval_secs` until `shutdown` fires.
pub fn spawn_collector(
    store: Arc<Store>,
    config: &Config,
    lock: CollectorLock,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let interval = Duration::from_secs(config.interval_secs.max(1));
    let labels = config.labels.clone();
    let sample_retention = chrono::Duration::hours(config.retention_hours as i64);
    let change_retention = chrono::Duration::days(config.change_retention_days as i64);
    std::thread::spawn(move || {
        let _lock = lock;
        let mut collectors = default_collectors();
        tracing::info!(
            collectors = ?collectors.iter().map(|c| c.name()).collect::<Vec<_>>(),
            interval_secs = interval.as_secs(),
            "collecting"
        );
        let mut last_prune = Instant::now();
        while !shutdown.is_cancelled() {
            let started = Instant::now();
            let n = collect_once(&store, &mut collectors, &labels, Some(interval.as_secs()));
            tracing::debug!(
                changes = n,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "pass"
            );

            if last_prune.elapsed() >= PRUNE_EVERY {
                let now = Utc::now();
                match store.prune(now - sample_retention, now - change_retention) {
                    Ok((s, c)) => tracing::info!(samples = s, changes = c, "pruned"),
                    Err(e) => tracing::warn!(error = %e, "prune failed"),
                }
                last_prune = Instant::now();
            }

            let deadline = started + interval;
            while !shutdown.is_cancelled() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    })
}
