//! Per-node journal cursors and routed action queue for the hub.
//!
//! A node pushes its own change rows (with its own `seq`) plus the current
//! resource bodies and samples. The hub copies new changes into its journal,
//! remembers the highest contiguous node seq, and never executes actions.

use chrono::{DateTime, Utc};
use pulseblade_core::{Change, ChangeKind, Health, LogLine, Resource, Severity};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::history::{self, MetricBucket, StoredEvent};
use crate::memory::FindingDraft;
use crate::{from_ms, Store, StoreError};

/// Pushes are expected about this often when a node does not say.
pub const DEFAULT_PUSH_INTERVAL_SECS: i64 = 15;
/// A node is stale after this many missed push intervals.
pub const STALE_MISSED_PUSHES: i64 = 3;
/// Finding rule for a node that stopped pushing. One open finding per host.
pub const OFFLINE_RULE: &str = "host offline";

/// One numeric sample with the timestamp the node observed it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StampedSample {
    pub resource_id: String,
    pub metric: String,
    pub ts: DateTime<Utc>,
    pub value: f64,
}

/// What a node pushes after the hub's cursor.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeBatch {
    #[serde(default)]
    pub changes: Vec<Change>,
    #[serde(default)]
    pub resources: Vec<Resource>,
    #[serde(default)]
    pub samples: Vec<StampedSample>,
    /// Closed 5-minute buckets. Raw samples in the open window stay in `samples`.
    #[serde(default)]
    pub buckets: Vec<MetricBucket>,
    /// Failures, restarts, and error lines. Not a journal dump.
    #[serde(default)]
    pub events: Vec<StoredEvent>,
    /// Seconds between pushes. Zero means [`DEFAULT_PUSH_INTERVAL_SECS`].
    #[serde(default)]
    pub interval_secs: u64,
    /// Set on the last push before a requested shutdown. A crash leaves this false.
    #[serde(default)]
    pub shutting_down: bool,
}

/// Highest node journal seq and sample timestamp the hub has applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCursor {
    pub seq: i64,
    pub sample_ms: i64,
    /// Highest closed bucket start the hub has stored for this node.
    #[serde(default)]
    pub bucket_ms: i64,
    /// Highest node event id the hub has stored.
    #[serde(default)]
    pub event_id: i64,
}

/// One row of `node_cursors`: how far the hub has applied this node, and when
/// it last accepted a push. `last_push_ms` is 0 for rows written before that
/// column existed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePush {
    pub node: String,
    pub seq: i64,
    pub last_push_ms: i64,
    /// Push interval the node reported, or the default when it did not.
    pub interval_secs: i64,
    /// The last accepted push was marked `shutting_down`.
    pub shutting_down: bool,
}

impl NodePush {
    /// `(age_secs, stale)`. Stale after [`STALE_MISSED_PUSHES`] intervals, or when
    /// the hub has never recorded a push time.
    pub fn freshness(&self, now: DateTime<Utc>) -> (i64, bool) {
        let interval = if self.interval_secs > 0 {
            self.interval_secs
        } else {
            DEFAULT_PUSH_INTERVAL_SECS
        };
        if self.last_push_ms <= 0 {
            return (STALE_MISSED_PUSHES * interval + 1, true);
        }
        let age = (now.timestamp_millis() - self.last_push_ms)
            .div_euclid(1000)
            .max(0);
        (age, age > STALE_MISSED_PUSHES * interval)
    }
}

/// How far the hub got after one push.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestResult {
    pub cursor: i64,
    pub sample_ms: i64,
    pub applied: usize,
}

/// A plan, execute, or status call waiting for the node that owns the resource.
#[derive(Debug, Clone, PartialEq)]
pub struct RouteJob {
    pub id: String,
    pub op: String,
    pub body: Value,
}

impl Store {
    pub fn node_cursor(&self, node_id: &str) -> crate::Result<NodeCursor> {
        let conn = self.conn();
        let row = conn
            .query_row(
                "SELECT seq, sample_ms, bucket_ms, event_id FROM node_cursors WHERE node_id = ?1",
                [node_id],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional_cursor()?;
        let (seq, sample_ms, bucket_ms, event_id) = row.unwrap_or((0, 0, 0, 0));
        Ok(NodeCursor {
            seq,
            sample_ms,
            bucket_ms,
            event_id,
        })
    }

    /// Every node that has pushed to this hub, by node id. Empty on a node.
    pub fn node_pushes(&self) -> crate::Result<Vec<NodePush>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT node_id, seq, last_push_ms, interval_secs, shutting_down
             FROM node_cursors ORDER BY node_id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(NodePush {
                node: r.get(0)?,
                seq: r.get(1)?,
                last_push_ms: r.get(2)?,
                interval_secs: r.get(3)?,
                shutting_down: r.get::<_, i64>(4)? != 0,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Copy `batch` into the fleet journal. Changes at or below the cursor are
    /// ignored. A seq that skips ahead is rejected and the cursor stays put.
    pub fn ingest_node(&self, node_id: &str, batch: &NodeBatch) -> crate::Result<IngestResult> {
        if node_id.is_empty() || node_id.contains(':') {
            return Err(StoreError::Invalid(
                "node id must be a non-empty name without ':'".into(),
            ));
        }
        let mut changes = batch.changes.clone();
        changes.sort_by_key(|c| c.seq);
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let (mut cursor, mut sample_ms, mut bucket_ms, mut event_id) = tx
            .query_row(
                "SELECT seq, sample_ms, bucket_ms, event_id FROM node_cursors WHERE node_id = ?1",
                [node_id],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional_cursor()?
            .unwrap_or((0, 0, 0, 0));
        let have = cursor;
        let mut applied = 0usize;
        for change in &changes {
            if change.seq <= have {
                continue;
            }
            if change.seq != cursor + 1 {
                return Err(StoreError::Gap {
                    node: node_id.to_string(),
                    have: cursor,
                    next: change.seq,
                });
            }
            let ms = change.ts.timestamp_millis();
            tx.execute(
                "INSERT INTO changes (ts, resource_id, kind, field, before, after)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    ms,
                    change.resource_id,
                    change.kind.as_str(),
                    change.field,
                    change.before.as_ref().map(Value::to_string),
                    change.after.as_ref().map(Value::to_string),
                ],
            )?;
            if change.kind == ChangeKind::Disappeared {
                tx.execute(
                    "UPDATE resources SET present = 0, last_seen = ?2 WHERE id = ?1",
                    params![change.resource_id, ms],
                )?;
            }
            cursor = change.seq;
            applied += 1;
        }
        let source = format!("node:{node_id}");
        for res in &batch.resources {
            let ms = Utc::now().timestamp_millis();
            tx.execute(
                "INSERT INTO resources
                    (id, kind, name, parent, health, labels, attrs, source, first_seen, last_seen, present)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9, 1)
                 ON CONFLICT(id) DO UPDATE SET
                    kind = excluded.kind, name = excluded.name, parent = excluded.parent,
                    health = excluded.health, labels = excluded.labels, attrs = excluded.attrs,
                    source = excluded.source, last_seen = excluded.last_seen, present = 1",
                params![
                    res.id,
                    res.kind.as_str(),
                    res.name,
                    res.parent,
                    res.health.as_str(),
                    serde_json::to_string(&res.labels)?,
                    serde_json::to_string(&res.attrs)?,
                    source,
                    ms,
                ],
            )?;
        }
        for sample in batch.samples.iter().filter(|s| s.value.is_finite()) {
            let ms = sample.ts.timestamp_millis();
            history::observe_sample(&tx, &sample.resource_id, &sample.metric, ms, sample.value)?;
            tx.execute(
                "INSERT INTO samples (resource_id, metric, ts, value) VALUES (?1, ?2, ?3, ?4)",
                params![sample.resource_id, sample.metric, ms, sample.value],
            )?;
            tx.execute(
                "INSERT INTO latest (resource_id, metric, ts, value) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(resource_id, metric) DO UPDATE SET
                    ts = excluded.ts, value = excluded.value
                 WHERE excluded.ts >= latest.ts",
                params![sample.resource_id, sample.metric, ms, sample.value],
            )?;
            if ms > sample_ms {
                sample_ms = ms;
            }
        }
        history::note_changes(&tx, &changes)?;
        for bucket in &batch.buckets {
            if bucket.bucket_ms > bucket_ms {
                bucket_ms = bucket.bucket_ms;
            }
        }
        // Raw samples already updated these buckets. A node bucket replaces the
        // row only when it has at least as many samples, so the two paths do not add.
        {
            let mut upsert = tx.prepare(
                "INSERT INTO sample_buckets (resource_id, metric, bucket_ms, n, vmin, vmax, vsum)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(resource_id, metric, bucket_ms) DO UPDATE SET
                    n = excluded.n, vmin = excluded.vmin, vmax = excluded.vmax, vsum = excluded.vsum
                 WHERE excluded.n >= sample_buckets.n",
            )?;
            for bucket in batch
                .buckets
                .iter()
                .filter(|b| b.n > 0 && b.sum.is_finite())
            {
                upsert.execute(params![
                    bucket.resource_id,
                    bucket.metric,
                    bucket.bucket_ms,
                    bucket.n as i64,
                    bucket.min,
                    bucket.max,
                    bucket.sum
                ])?;
            }
        }
        for event in &batch.events {
            if event.id > event_id {
                event_id = event.id;
            }
            history::store_pushed(&tx, event)?;
        }
        let interval = if batch.interval_secs == 0 {
            DEFAULT_PUSH_INTERVAL_SECS
        } else {
            batch.interval_secs as i64
        };
        let pushed_ms = Utc::now().timestamp_millis();
        tx.execute(
            "INSERT INTO node_cursors
                (node_id, seq, sample_ms, last_push_ms, interval_secs, shutting_down, bucket_ms, event_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(node_id) DO UPDATE SET
                seq = excluded.seq,
                sample_ms = excluded.sample_ms,
                last_push_ms = excluded.last_push_ms,
                interval_secs = excluded.interval_secs,
                shutting_down = excluded.shutting_down,
                bucket_ms = excluded.bucket_ms,
                event_id = excluded.event_id",
            params![
                node_id,
                cursor,
                sample_ms,
                pushed_ms,
                interval,
                i64::from(batch.shutting_down),
                bucket_ms,
                event_id
            ],
        )?;
        tx.commit()?;
        Ok(IngestResult {
            cursor,
            sample_ms,
            applied,
        })
    }

    /// Samples with `ts` strictly after `after_ms`, oldest first.
    pub fn samples_since(&self, after_ms: i64, limit: usize) -> crate::Result<Vec<StampedSample>> {
        let limit = limit.clamp(1, 5_000) as i64;
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT resource_id, metric, ts, value FROM samples
             WHERE ts > ?1 ORDER BY ts LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after_ms, limit], |r| {
            Ok(StampedSample {
                resource_id: r.get(0)?,
                metric: r.get(1)?,
                ts: from_ms(r.get(2)?),
                value: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Move the recorded push time. Silence is `now` minus several intervals.
    pub fn set_node_last_push(&self, node_id: &str, last_push_ms: i64) -> crate::Result<bool> {
        let n = self.conn().execute(
            "UPDATE node_cursors SET last_push_ms = ?2 WHERE node_id = ?1",
            params![node_id, last_push_ms],
        )?;
        Ok(n == 1)
    }

    /// One open `host offline` finding per stale node. A push that is fresh again
    /// resolves that host's finding and leaves every other finding alone.
    /// Resources are not marked disappeared.
    pub fn sync_offline_hosts(
        &self,
        now: DateTime<Utc>,
    ) -> crate::Result<Vec<pulseblade_core::Finding>> {
        let mut drafts = Vec::new();
        for push in self.node_pushes()? {
            let (age, stale) = push.freshness(now);
            if !stale {
                continue;
            }
            let host_id = format!("host:{}", push.node);
            let health = self
                .resource(&host_id)?
                .map(|row| row.resource.health)
                .unwrap_or(Health::Unknown);
            let ids = self.host_scope(&host_id)?;
            let context = self.recent_changes(&ids, 8)?;
            let logs = self.host_error_lines(&ids, 5)?;
            let summary = if push.shutting_down {
                format!("{} shut down (last health {})", push.node, health.as_str())
            } else {
                format!(
                    "{} offline (last health {}); no push for {age}s",
                    push.node,
                    health.as_str()
                )
            };
            drafts.push(FindingDraft {
                dedupe_key: format!("{OFFLINE_RULE}\u{0}{host_id}"),
                resource_id: host_id,
                rule: OFFLINE_RULE.to_string(),
                severity: if push.shutting_down {
                    Severity::Warning
                } else {
                    Severity::Critical
                },
                summary,
                context,
                logs,
            });
        }
        self.reconcile_rule(OFFLINE_RULE, &drafts, now)
    }

    fn host_scope(&self, host_id: &str) -> crate::Result<Vec<String>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id FROM resources
             WHERE present = 1 AND (id = ?1 OR parent = ?1)
             ORDER BY id LIMIT 32",
        )?;
        let rows = stmt.query_map([host_id], |r| r.get::<_, String>(0))?;
        let mut ids: Vec<String> = rows.collect::<rusqlite::Result<_>>()?;
        if !ids.iter().any(|id| id == host_id) {
            ids.insert(0, host_id.to_string());
        }
        Ok(ids)
    }

    fn host_error_lines(&self, ids: &[String], limit: usize) -> crate::Result<Vec<LogLine>> {
        let mut logs = self.recent_logs(ids, limit)?;
        if logs.len() < limit {
            let more = self.error_events(ids, limit - logs.len())?;
            for line in more {
                if !logs.iter().any(|have| have.message == line.message) {
                    logs.push(line);
                }
            }
        }
        Ok(logs)
    }

    pub fn enqueue_route(
        &self,
        id: &str,
        node_id: &str,
        op: &str,
        body: &Value,
    ) -> crate::Result<()> {
        self.conn().execute(
            "INSERT INTO route_rpc (id, node_id, op, body, status, result, created_at)
             VALUES (?1, ?2, ?3, ?4, 'queued', NULL, ?5)",
            params![
                id,
                node_id,
                op,
                body.to_string(),
                Utc::now().timestamp_millis()
            ],
        )?;
        Ok(())
    }

    pub fn poll_routes(&self, node_id: &str, limit: usize) -> crate::Result<Vec<RouteJob>> {
        let limit = limit.clamp(1, 50) as i64;
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, op, body FROM route_rpc
             WHERE node_id = ?1 AND status = 'queued'
             ORDER BY created_at LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![node_id, limit], |r| {
            let body: String = r.get(2)?;
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, body))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id, op, body) = row?;
            out.push(RouteJob {
                id,
                op,
                body: serde_json::from_str(&body)?,
            });
        }
        Ok(out)
    }

    pub fn finish_route(&self, id: &str, node_id: &str, result: &Value) -> crate::Result<bool> {
        let n = self.conn().execute(
            "UPDATE route_rpc SET status = 'done', result = ?3
             WHERE id = ?1 AND node_id = ?2 AND status = 'queued'",
            params![id, node_id, result.to_string()],
        )?;
        Ok(n == 1)
    }

    pub fn remember_plan(&self, plan_id: &str, node_id: &str) -> crate::Result<()> {
        self.conn().execute(
            "INSERT INTO route_plans (plan_id, node_id) VALUES (?1, ?2)
             ON CONFLICT(plan_id) DO UPDATE SET node_id = excluded.node_id",
            params![plan_id, node_id],
        )?;
        Ok(())
    }

    pub fn plan_node(&self, plan_id: &str) -> crate::Result<Option<String>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT node_id FROM route_plans WHERE plan_id = ?1",
                [plan_id],
                |r| r.get(0),
            )
            .optional_cursor()?)
    }

    pub fn cache_run(&self, run_id: &str, node_id: &str, body: &Value) -> crate::Result<()> {
        self.conn().execute(
            "INSERT INTO route_results (id, node_id, body) VALUES (?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET node_id = excluded.node_id, body = excluded.body",
            params![run_id, node_id, body.to_string()],
        )?;
        Ok(())
    }

    pub fn cached_run(&self, run_id: &str) -> crate::Result<Option<(String, Value)>> {
        let conn = self.conn();
        let row = conn
            .query_row(
                "SELECT node_id, body FROM route_results WHERE id = ?1",
                [run_id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional_cursor()?;
        match row {
            Some((node, body)) => Ok(Some((node, serde_json::from_str(&body)?))),
            None => Ok(None),
        }
    }
}

trait OptionalCursor<T> {
    fn optional_cursor(self) -> Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalCursor<T> for Result<T, rusqlite::Error> {
    fn optional_cursor(self) -> Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulseblade_core::{Health, ResourceKind};

    fn change(seq: i64, id: &str, kind: ChangeKind) -> Change {
        Change {
            seq,
            ts: Utc::now(),
            resource_id: id.into(),
            kind,
            field: None,
            before: None,
            after: None,
        }
    }

    #[test]
    fn catch_up_is_contiguous_and_a_gap_does_not_advance() {
        let store = Store::open_in_memory().unwrap();
        let host = Resource::new("host:web1", ResourceKind::Host, "web1").health(Health::Ok);
        let first = NodeBatch {
            changes: vec![change(1, "host:web1", ChangeKind::Appeared)],
            resources: vec![host.clone()],
            samples: vec![StampedSample {
                resource_id: "host:web1".into(),
                metric: "cpu.used_pct".into(),
                ts: Utc::now(),
                value: 10.0,
            }],
            ..NodeBatch::default()
        };
        let got = store.ingest_node("web1", &first).unwrap();
        assert_eq!(got.applied, 1);
        assert_eq!(got.cursor, 1);
        let again = store.ingest_node("web1", &first).unwrap();
        assert_eq!(again.applied, 0);
        assert_eq!(again.cursor, 1);

        let skipped = NodeBatch {
            changes: vec![change(3, "host:web1", ChangeKind::Changed)],
            ..NodeBatch::default()
        };
        let err = store.ingest_node("web1", &skipped).unwrap_err();
        assert!(matches!(
            err,
            StoreError::Gap {
                have: 1,
                next: 3,
                ..
            }
        ));
        assert_eq!(store.node_cursor("web1").unwrap().seq, 1);

        let rest = NodeBatch {
            changes: vec![
                change(2, "unit:web1:sshd.service", ChangeKind::Appeared),
                change(3, "unit:web1:sshd.service", ChangeKind::Changed),
            ],
            resources: vec![
                host,
                Resource::new(
                    "unit:web1:sshd.service",
                    ResourceKind::Service,
                    "sshd.service",
                )
                .parent("host:web1")
                .health(Health::Failed),
            ],
            ..NodeBatch::default()
        };
        let got = store.ingest_node("web1", &rest).unwrap();
        assert_eq!(got.applied, 2);
        assert_eq!(got.cursor, 3);
        assert_eq!(store.current_seq().unwrap(), 3);
        let pushes = store.node_pushes().unwrap();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].node, "web1");
        assert_eq!(pushes[0].seq, 3);
        assert!(pushes[0].last_push_ms > 0);
        let unit = store.resource("unit:web1:sshd.service").unwrap().unwrap();
        assert_eq!(unit.resource.health, Health::Failed);
        assert!(unit.present);
    }

    #[test]
    fn old_cursor_table_gains_last_push() {
        let path = std::env::temp_dir().join(format!(
            "pulseblade-cursor-migrate-{}-{}.db",
            std::process::id(),
            Utc::now().timestamp_millis()
        ));
        let _ = std::fs::remove_file(&path);
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE node_cursors (
                    node_id TEXT PRIMARY KEY,
                    seq INTEGER NOT NULL,
                    sample_ms INTEGER NOT NULL
                );
                INSERT INTO node_cursors (node_id, seq, sample_ms) VALUES ('web1', 4, 10);",
            )
            .unwrap();
        }
        let store = Store::open(&path).unwrap();
        let pushes = store.node_pushes().unwrap();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].node, "web1");
        assert_eq!(pushes[0].seq, 4);
        assert_eq!(pushes[0].last_push_ms, 0);
        assert_eq!(pushes[0].interval_secs, 15);
        assert!(!pushes[0].shutting_down);
        drop(store);
        for suffix in ["", "-wal", "-shm"] {
            let mut sidecar = path.clone().into_os_string();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(sidecar);
        }
    }

    #[test]
    fn silence_keeps_the_host_and_one_offline_finding() {
        use pulseblade_core::{FindingStatus, LogLine, Observation};

        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        store
            .ingest_node(
                "web1",
                &NodeBatch {
                    changes: vec![change(1, "host:web1", ChangeKind::Appeared)],
                    resources: vec![Resource::new("host:web1", ResourceKind::Host, "web1")
                        .health(Health::Degraded)],
                    interval_secs: 15,
                    ..NodeBatch::default()
                },
            )
            .unwrap();
        store
            .apply(
                "journald",
                now,
                &Observation {
                    resources: vec![],
                    samples: vec![],
                    logs: vec![LogLine {
                        ts: now,
                        resource_id: "host:web1".into(),
                        message: "link down on eth0".into(),
                    }],
                },
            )
            .unwrap();
        store
            .reconcile_findings(
                &[FindingDraft {
                    dedupe_key: "health\u{0}unit:web1:sshd".into(),
                    resource_id: "unit:web1:sshd".into(),
                    rule: "health".into(),
                    severity: pulseblade_core::Severity::Warning,
                    summary: "sshd degraded".into(),
                    context: vec![],
                    logs: vec![],
                }],
                now,
            )
            .unwrap();

        let (age, stale) = store.node_pushes().unwrap()[0].freshness(now);
        assert!(age < 5, "{age}");
        assert!(!stale);
        assert!(store.sync_offline_hosts(now).unwrap().is_empty());

        let silent_at = now.timestamp_millis() - 46_000;
        assert!(store.set_node_last_push("web1", silent_at).unwrap());
        let opened = store.sync_offline_hosts(now).unwrap();
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].rule, OFFLINE_RULE);
        assert!(opened[0].summary.contains("last health degraded"));
        assert!(opened[0].summary.contains("offline"));
        assert!(opened[0].logs.iter().any(|l| l.message.contains("eth0")));
        assert!(store.sync_offline_hosts(now).unwrap().is_empty());
        let (open, total) = store
            .findings(
                &crate::FindingFilter {
                    status: Some(FindingStatus::Open),
                    ..Default::default()
                },
                10,
            )
            .unwrap();
        assert_eq!(total, 2, "the health finding stays open");
        assert_eq!(open.iter().filter(|f| f.rule == OFFLINE_RULE).count(), 1);
        assert!(store.resource("host:web1").unwrap().unwrap().present);

        store
            .ingest_node(
                "web1",
                &NodeBatch {
                    interval_secs: 15,
                    shutting_down: false,
                    ..NodeBatch::default()
                },
            )
            .unwrap();
        store.sync_offline_hosts(Utc::now()).unwrap();
        let (still, _) = store
            .findings(
                &crate::FindingFilter {
                    status: Some(FindingStatus::Open),
                    resource_id: Some("host:web1".into()),
                    ..Default::default()
                },
                10,
            )
            .unwrap();
        assert!(still.is_empty(), "{still:?}");
        assert!(store.resource("host:web1").unwrap().unwrap().present);
    }

    #[test]
    fn shutting_down_is_not_a_disappearance() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        store
            .ingest_node(
                "web1",
                &NodeBatch {
                    changes: vec![change(1, "host:web1", ChangeKind::Appeared)],
                    resources: vec![
                        Resource::new("host:web1", ResourceKind::Host, "web1").health(Health::Ok)
                    ],
                    interval_secs: 10,
                    shutting_down: true,
                    ..NodeBatch::default()
                },
            )
            .unwrap();
        let push = &store.node_pushes().unwrap()[0];
        assert!(push.shutting_down);
        assert!(!push.freshness(now).1);
        store
            .set_node_last_push("web1", now.timestamp_millis() - 31_000)
            .unwrap();
        let opened = store.sync_offline_hosts(now).unwrap();
        assert_eq!(opened.len(), 1);
        assert!(opened[0].summary.contains("shut down"));
        assert_eq!(opened[0].severity, pulseblade_core::Severity::Warning);
        assert!(store.resource("host:web1").unwrap().unwrap().present);
        let (changes, _) = store
            .changes_after(0, &crate::ChangeFilter::default(), 20)
            .unwrap();
        assert!(!changes.iter().any(|c| c.kind == ChangeKind::Disappeared));
    }
}
