//! Per-node journal cursors and routed action queue for the hub.
//!
//! A node pushes its own change rows (with its own `seq`) plus the current
//! resource bodies and samples. The hub copies new changes into its journal,
//! remembers the highest contiguous node seq, and never executes actions.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use pulseblade_core::{Change, ChangeKind, Health, LogLine, Resource, Severity};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::history::{self, MetricBucket, StoredEvent};
use crate::memory::FindingDraft;
use crate::sql::{values_sql, Sql, SqlExt, SqlVal};
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

fn ingest_batch(
    sql: &mut dyn Sql,
    node_id: &str,
    batch: &NodeBatch,
    changes: &[Change],
    interval: i64,
    pushed_ms: i64,
) -> crate::Result<IngestResult> {
    let select = if sql.postgres() {
        "SELECT seq, sample_ms, bucket_ms, event_id FROM node_cursors WHERE node_id = ?1 FOR UPDATE"
    } else {
        "SELECT seq, sample_ms, bucket_ms, event_id FROM node_cursors WHERE node_id = ?1"
    };
    let (mut cursor, mut sample_ms, mut bucket_ms, mut event_id) = sql
        .query_opt(select, &[SqlVal::text(node_id)], |row| {
            Ok((row.int(0)?, row.int(1)?, row.int(2)?, row.int(3)?))
        })?
        .unwrap_or((0, 0, 0, 0));
    let have = cursor;
    let mut applied = 0usize;
    let mut accepted = Vec::new();
    for change in changes {
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
        cursor = change.seq;
        applied += 1;
        accepted.push(change.clone());
    }
    write_rows(
        sql,
        "INSERT INTO changes (ts, resource_id, kind, field, before, after) VALUES ",
        "",
        6,
        &accepted
            .iter()
            .map(|change| {
                vec![
                    SqlVal::Int(change.ts.timestamp_millis()),
                    SqlVal::text(&change.resource_id),
                    SqlVal::text(change.kind.as_str()),
                    SqlVal::opt_text(change.field.clone()),
                    SqlVal::opt_text(change.before.as_ref().map(Value::to_string)),
                    SqlVal::opt_text(change.after.as_ref().map(Value::to_string)),
                ]
            })
            .collect::<Vec<_>>(),
    )?;
    for change in &accepted {
        if change.kind == ChangeKind::Disappeared {
            sql.execute(
                "UPDATE resources SET present = 0, last_seen = ?2 WHERE id = ?1",
                &[
                    SqlVal::text(&change.resource_id),
                    SqlVal::Int(change.ts.timestamp_millis()),
                ],
            )?;
        }
    }
    let source = format!("node:{node_id}");
    let seen_ms = Utc::now().timestamp_millis();
    let mut resources = Vec::new();
    let mut seen_ids = std::collections::BTreeSet::new();
    for res in batch.resources.iter().rev() {
        if seen_ids.insert(res.id.clone()) {
            resources.push(res);
        }
    }
    resources.reverse();
    write_rows(
        sql,
        "INSERT INTO resources
            (id, kind, name, parent, health, labels, attrs, source, first_seen, last_seen, present)
         VALUES ",
        " ON CONFLICT(id) DO UPDATE SET
            kind = excluded.kind, name = excluded.name, parent = excluded.parent,
            health = excluded.health, labels = excluded.labels, attrs = excluded.attrs,
            source = excluded.source, last_seen = excluded.last_seen, present = 1",
        11,
        &resources
            .iter()
            .map(|res| {
                vec![
                    SqlVal::text(&res.id),
                    SqlVal::text(res.kind.as_str()),
                    SqlVal::text(&res.name),
                    SqlVal::opt_text(res.parent.clone()),
                    SqlVal::text(res.health.as_str()),
                    SqlVal::text(
                        serde_json::to_string(&res.labels).unwrap_or_else(|_| "{}".into()),
                    ),
                    SqlVal::text(serde_json::to_string(&res.attrs).unwrap_or_else(|_| "{}".into())),
                    SqlVal::text(&source),
                    SqlVal::Int(seen_ms),
                    SqlVal::Int(seen_ms),
                    SqlVal::Int(1),
                ]
            })
            .collect::<Vec<_>>(),
    )?;

    let samples: Vec<&StampedSample> = batch
        .samples
        .iter()
        .filter(|sample| sample.value.is_finite())
        .collect();
    note_restarts(sql, &samples)?;
    write_rows(
        sql,
        "INSERT INTO samples (resource_id, metric, ts, value) VALUES ",
        "",
        4,
        &samples
            .iter()
            .map(|sample| {
                let ms = sample.ts.timestamp_millis();
                if ms > sample_ms {
                    sample_ms = ms;
                }
                vec![
                    SqlVal::text(&sample.resource_id),
                    SqlVal::text(&sample.metric),
                    SqlVal::Int(ms),
                    SqlVal::Float(sample.value),
                ]
            })
            .collect::<Vec<_>>(),
    )?;
    let mut latest: HashMap<(String, String), (i64, f64)> = HashMap::new();
    for sample in &samples {
        let ms = sample.ts.timestamp_millis();
        let key = (sample.resource_id.clone(), sample.metric.clone());
        latest
            .entry(key)
            .and_modify(|(ts, value)| {
                if ms >= *ts {
                    *ts = ms;
                    *value = sample.value;
                }
            })
            .or_insert((ms, sample.value));
    }
    write_rows(
        sql,
        "INSERT INTO latest (resource_id, metric, ts, value) VALUES ",
        " ON CONFLICT(resource_id, metric) DO UPDATE SET
            ts = excluded.ts, value = excluded.value
          WHERE excluded.ts >= latest.ts",
        4,
        &latest
            .iter()
            .map(|((resource, metric), (ts, value))| {
                vec![
                    SqlVal::text(resource),
                    SqlVal::text(metric),
                    SqlVal::Int(*ts),
                    SqlVal::Float(*value),
                ]
            })
            .collect::<Vec<_>>(),
    )?;
    fold_sample_buckets(sql, &samples)?;
    history::note_changes_sql(sql, &accepted)?;

    let mut pushed_buckets: HashMap<(String, String, i64), &MetricBucket> = HashMap::new();
    for bucket in batch
        .buckets
        .iter()
        .filter(|bucket| bucket.n > 0 && bucket.sum.is_finite())
    {
        if bucket.bucket_ms > bucket_ms {
            bucket_ms = bucket.bucket_ms;
        }
        pushed_buckets.insert(
            (
                bucket.resource_id.clone(),
                bucket.metric.clone(),
                bucket.bucket_ms,
            ),
            bucket,
        );
    }
    write_rows(
        sql,
        "INSERT INTO sample_buckets (resource_id, metric, bucket_ms, n, vmin, vmax, vsum) VALUES ",
        " ON CONFLICT(resource_id, metric, bucket_ms) DO UPDATE SET
            n = excluded.n, vmin = excluded.vmin, vmax = excluded.vmax, vsum = excluded.vsum
          WHERE excluded.n >= sample_buckets.n",
        7,
        &pushed_buckets
            .values()
            .map(|bucket| {
                vec![
                    SqlVal::text(&bucket.resource_id),
                    SqlVal::text(&bucket.metric),
                    SqlVal::Int(bucket.bucket_ms),
                    SqlVal::Int(bucket.n as i64),
                    SqlVal::Float(bucket.min),
                    SqlVal::Float(bucket.max),
                    SqlVal::Float(bucket.sum),
                ]
            })
            .collect::<Vec<_>>(),
    )?;
    for event in &batch.events {
        if event.id > event_id {
            event_id = event.id;
        }
    }
    let event_rows: Vec<Vec<SqlVal>> = batch
        .events
        .iter()
        .map(|event| {
            vec![
                SqlVal::Int(event.ts.timestamp_millis()),
                SqlVal::text(&event.resource_id),
                SqlVal::text(&event.kind),
                SqlVal::text(&event.severity),
                SqlVal::text(&event.message),
            ]
        })
        .collect();
    if sql.postgres() {
        write_rows(
            sql,
            "INSERT INTO events (ts, resource_id, kind, severity, message) VALUES ",
            " ON CONFLICT(resource_id, ts, kind, message) DO NOTHING",
            5,
            &event_rows,
        )?;
    } else {
        // RETURNING is only the rows this statement inserted, so the FTS index
        // does not gain a second entry when the node retries a batch.
        for chunk in event_rows.chunks(80) {
            let mut statement = values_sql(
                "INSERT INTO events (ts, resource_id, kind, severity, message) VALUES ",
                chunk.len(),
                5,
            );
            statement.push_str(
                " ON CONFLICT(resource_id, ts, kind, message) DO NOTHING
                 RETURNING id, message, resource_id",
            );
            let args: Vec<SqlVal> = chunk.iter().flatten().cloned().collect();
            let inserted = sql.query(&statement, &args, |row| {
                Ok((row.int(0)?, row.text(1)?, row.text(2)?))
            })?;
            for (id, message, resource) in inserted {
                sql.execute(
                    "INSERT INTO events_fts(rowid, message, resource_id) VALUES (?1, ?2, ?3)",
                    &[
                        SqlVal::Int(id),
                        SqlVal::text(message),
                        SqlVal::text(resource),
                    ],
                )?;
            }
        }
    }
    sql.execute(
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
        &[
            SqlVal::text(node_id),
            SqlVal::Int(cursor),
            SqlVal::Int(sample_ms),
            SqlVal::Int(pushed_ms),
            SqlVal::Int(interval),
            SqlVal::Int(i64::from(batch.shutting_down)),
            SqlVal::Int(bucket_ms),
            SqlVal::Int(event_id),
        ],
    )?;
    Ok(IngestResult {
        cursor,
        sample_ms,
        applied,
    })
}

fn note_restarts(sql: &mut dyn Sql, samples: &[&StampedSample]) -> crate::Result<()> {
    let mut restarts: Vec<&StampedSample> = samples
        .iter()
        .copied()
        .filter(|sample| history::is_restart_metric(&sample.metric))
        .collect();
    if restarts.is_empty() {
        return Ok(());
    }
    restarts.sort_by_key(|sample| sample.ts);
    let known = sql.query(
        "SELECT resource_id, metric, value FROM latest
         WHERE metric = 'k8s.restarts' OR metric LIKE '%.restarts'",
        &[],
        |row| Ok((row.text(0)?, row.text(1)?, row.float(2)?)),
    )?;
    let mut prev: HashMap<(String, String), f64> = known
        .into_iter()
        .map(|(resource, metric, value)| ((resource, metric), value))
        .collect();
    for sample in restarts {
        let key = (sample.resource_id.clone(), sample.metric.clone());
        if prev.get(&key).is_some_and(|was| sample.value > *was) {
            history::insert_event_sql(
                sql,
                sample.ts.timestamp_millis(),
                &sample.resource_id,
                "restart",
                "warning",
                &format!("restart count {}", sample.value),
            )?;
        }
        prev.insert(key, sample.value);
    }
    Ok(())
}

fn fold_sample_buckets(sql: &mut dyn Sql, samples: &[&StampedSample]) -> crate::Result<()> {
    struct Acc {
        n: i64,
        min: f64,
        max: f64,
        sum: f64,
    }
    let mut buckets: HashMap<(String, String, i64), Acc> = HashMap::new();
    for sample in samples {
        let start = history::bucket_start(sample.ts.timestamp_millis());
        let key = (sample.resource_id.clone(), sample.metric.clone(), start);
        buckets
            .entry(key)
            .and_modify(|acc| {
                acc.n += 1;
                acc.min = acc.min.min(sample.value);
                acc.max = acc.max.max(sample.value);
                acc.sum += sample.value;
            })
            .or_insert(Acc {
                n: 1,
                min: sample.value,
                max: sample.value,
                sum: sample.value,
            });
    }
    let (least, greatest) = if sql.postgres() {
        ("LEAST", "GREATEST")
    } else {
        ("MIN", "MAX")
    };
    write_rows(
        sql,
        "INSERT INTO sample_buckets (resource_id, metric, bucket_ms, n, vmin, vmax, vsum) VALUES ",
        &format!(
            " ON CONFLICT(resource_id, metric, bucket_ms) DO UPDATE SET
            n = sample_buckets.n + excluded.n,
            vmin = {least}(sample_buckets.vmin, excluded.vmin),
            vmax = {greatest}(sample_buckets.vmax, excluded.vmax),
            vsum = sample_buckets.vsum + excluded.vsum"
        ),
        7,
        &buckets
            .iter()
            .map(|((resource, metric, bucket), acc)| {
                vec![
                    SqlVal::text(resource),
                    SqlVal::text(metric),
                    SqlVal::Int(*bucket),
                    SqlVal::Int(acc.n),
                    SqlVal::Float(acc.min),
                    SqlVal::Float(acc.max),
                    SqlVal::Float(acc.sum),
                ]
            })
            .collect::<Vec<_>>(),
    )
}

fn write_rows(
    sql: &mut dyn Sql,
    head: &str,
    tail: &str,
    cols: usize,
    rows: &[Vec<SqlVal>],
) -> crate::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    for chunk in rows.chunks(80) {
        let mut statement = values_sql(head, chunk.len(), cols);
        statement.push_str(tail);
        let args: Vec<SqlVal> = chunk.iter().flatten().cloned().collect();
        sql.execute(&statement, &args)?;
    }
    Ok(())
}

impl Store {
    pub fn node_cursor(&self, node_id: &str) -> crate::Result<NodeCursor> {
        let row = self.with_sql(|sql| {
            sql.query_opt(
                "SELECT seq, sample_ms, bucket_ms, event_id FROM node_cursors WHERE node_id = ?1",
                &[SqlVal::text(node_id)],
                |row| Ok((row.int(0)?, row.int(1)?, row.int(2)?, row.int(3)?)),
            )
        })?;
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
        self.with_sql(|sql| {
            sql.query(
                "SELECT node_id, seq, last_push_ms, interval_secs, shutting_down
                 FROM node_cursors ORDER BY node_id",
                &[],
                |row| {
                    Ok(NodePush {
                        node: row.text(0)?,
                        seq: row.int(1)?,
                        last_push_ms: row.int(2)?,
                        interval_secs: row.int(3)?,
                        shutting_down: row.int(4)? != 0,
                    })
                },
            )
        })
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
        let interval = if batch.interval_secs == 0 {
            DEFAULT_PUSH_INTERVAL_SECS
        } else {
            batch.interval_secs as i64
        };
        let pushed_ms = Utc::now().timestamp_millis();
        // The check-in commits before the sample write. A node waiting on that
        // write is still reporting, so the offline rule does not fire for it.
        self.with_sql(|sql| {
            sql.execute(
                "INSERT INTO node_cursors
                    (node_id, seq, sample_ms, last_push_ms, interval_secs, shutting_down, bucket_ms, event_id)
                 VALUES (?1, 0, 0, ?2, ?3, ?4, 0, 0)
                 ON CONFLICT(node_id) DO UPDATE SET
                    last_push_ms = excluded.last_push_ms,
                    interval_secs = excluded.interval_secs,
                    shutting_down = excluded.shutting_down",
                &[
                    SqlVal::text(node_id),
                    SqlVal::Int(pushed_ms),
                    SqlVal::Int(interval),
                    SqlVal::Int(i64::from(batch.shutting_down)),
                ],
            )?;
            Ok(())
        })?;
        self.transaction(|sql| ingest_batch(sql, node_id, batch, &changes, interval, pushed_ms))
    }

    /// Samples with `ts` strictly after `after_ms`, oldest first.
    pub fn samples_since(&self, after_ms: i64, limit: usize) -> crate::Result<Vec<StampedSample>> {
        let limit = limit.clamp(1, 5_000) as i64;
        self.with_sql(|sql| {
            sql.query(
                "SELECT resource_id, metric, ts, value FROM samples
                 WHERE ts > ?1 ORDER BY ts LIMIT ?2",
                &[SqlVal::Int(after_ms), SqlVal::Int(limit)],
                |row| {
                    Ok(StampedSample {
                        resource_id: row.text(0)?,
                        metric: row.text(1)?,
                        ts: from_ms(row.int(2)?),
                        value: row.float(3)?,
                    })
                },
            )
        })
    }

    /// Move the recorded push time. Silence is `now` minus several intervals.
    pub fn set_node_last_push(&self, node_id: &str, last_push_ms: i64) -> crate::Result<bool> {
        let n = self.with_sql(|sql| {
            sql.execute(
                "UPDATE node_cursors SET last_push_ms = ?2 WHERE node_id = ?1",
                &[SqlVal::text(node_id), SqlVal::Int(last_push_ms)],
            )
        })?;
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
        let mut ids = self.with_sql(|sql| {
            sql.query(
                "SELECT id FROM resources
                 WHERE present = 1 AND (id = ?1 OR parent = ?1)
                 ORDER BY id LIMIT 32",
                &[SqlVal::text(host_id)],
                |row| row.text(0),
            )
        })?;
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
        self.with_sql(|sql| {
            sql.execute(
                "INSERT INTO route_rpc (id, node_id, op, body, status, result, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'queued', NULL, ?5)",
                &[
                    SqlVal::text(id),
                    SqlVal::text(node_id),
                    SqlVal::text(op),
                    SqlVal::text(body.to_string()),
                    SqlVal::Int(Utc::now().timestamp_millis()),
                ],
            )?;
            Ok(())
        })
    }

    pub fn poll_routes(&self, node_id: &str, limit: usize) -> crate::Result<Vec<RouteJob>> {
        let limit = limit.clamp(1, 50) as i64;
        let rows = self.with_sql(|sql| {
            sql.query(
                "SELECT id, op, body FROM route_rpc
                 WHERE node_id = ?1 AND status = 'queued'
                 ORDER BY created_at LIMIT ?2",
                &[SqlVal::text(node_id), SqlVal::Int(limit)],
                |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
            )
        })?;
        let mut out = Vec::new();
        for (id, op, body) in rows {
            out.push(RouteJob {
                id,
                op,
                body: serde_json::from_str(&body)?,
            });
        }
        Ok(out)
    }

    pub fn finish_route(&self, id: &str, node_id: &str, result: &Value) -> crate::Result<bool> {
        let n = self.with_sql(|sql| {
            sql.execute(
                "UPDATE route_rpc SET status = 'done', result = ?3
                 WHERE id = ?1 AND node_id = ?2 AND status = 'queued'",
                &[
                    SqlVal::text(id),
                    SqlVal::text(node_id),
                    SqlVal::text(result.to_string()),
                ],
            )
        })?;
        Ok(n == 1)
    }

    pub fn remember_plan(&self, plan_id: &str, node_id: &str) -> crate::Result<()> {
        self.with_sql(|sql| {
            sql.execute(
                "INSERT INTO route_plans (plan_id, node_id) VALUES (?1, ?2)
                 ON CONFLICT(plan_id) DO UPDATE SET node_id = excluded.node_id",
                &[SqlVal::text(plan_id), SqlVal::text(node_id)],
            )?;
            Ok(())
        })
    }

    pub fn plan_node(&self, plan_id: &str) -> crate::Result<Option<String>> {
        self.with_sql(|sql| {
            sql.query_opt(
                "SELECT node_id FROM route_plans WHERE plan_id = ?1",
                &[SqlVal::text(plan_id)],
                |row| row.text(0),
            )
        })
    }

    pub fn cache_run(&self, run_id: &str, node_id: &str, body: &Value) -> crate::Result<()> {
        self.with_sql(|sql| {
            sql.execute(
                "INSERT INTO route_results (id, node_id, body) VALUES (?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET node_id = excluded.node_id, body = excluded.body",
                &[
                    SqlVal::text(run_id),
                    SqlVal::text(node_id),
                    SqlVal::text(body.to_string()),
                ],
            )?;
            Ok(())
        })
    }

    pub fn cached_run(&self, run_id: &str) -> crate::Result<Option<(String, Value)>> {
        let row = self.with_sql(|sql| {
            sql.query_opt(
                "SELECT node_id, body FROM route_results WHERE id = ?1",
                &[SqlVal::text(run_id)],
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
        })?;
        match row {
            Some((node, body)) => Ok(Some((node, serde_json::from_str(&body)?))),
            None => Ok(None),
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

    fn assert_contiguous_catch_up(store: &Store) {
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
    fn catch_up_is_contiguous_and_a_gap_does_not_advance() {
        assert_contiguous_catch_up(&Store::open_in_memory().unwrap());
    }

    /// Same ingest as the sqlite test. Skipped unless `PULSEBLADE_TEST_PG` is a
    /// disposable `postgres://` URL with no TLS.
    #[test]
    fn postgres_catch_up_matches_sqlite_when_configured() {
        let Ok(url) = std::env::var("PULSEBLADE_TEST_PG") else {
            return;
        };
        if url.trim().is_empty() {
            return;
        }
        let store = Store::open_postgres(&url, None).unwrap();
        store
            .with_sql(|sql| {
                sql.batch(
                    "TRUNCATE resources, changes, samples, latest, sample_buckets, events, node_cursors RESTART IDENTITY",
                )
            })
            .unwrap();
        assert_contiguous_catch_up(&store);
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
