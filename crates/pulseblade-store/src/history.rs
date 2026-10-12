//! Five-minute metric buckets and the event index.
//!
//! Raw samples stay on their own retention clock. Each sample also updates a
//! 5-minute min/max/avg bucket so the longer window survives after raw rows are
//! deleted. Failures, restarts, and error lines are events; info logs are not.

use chrono::{DateTime, Utc};
use pulseblade_core::{Change, ChangeKind};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::{from_ms, Store};

/// Width of a rolled-up metric bucket.
pub const BUCKET_MS: i64 = 300_000;
/// Raw samples kept on a hub. A node's raw window stays `retention_hours`.
pub const HUB_RAW_HOURS: i64 = 48;
/// Buckets and events kept when config does not say otherwise.
pub const DEFAULT_HISTORY_DAYS: i64 = 90;

/// One closed or open 5-minute bucket for a series.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricBucket {
    pub resource_id: String,
    pub metric: String,
    pub bucket_ms: i64,
    pub n: u64,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
}

impl MetricBucket {
    pub fn avg(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            self.sum / self.n as f64
        }
    }
}

/// A failure, restart, or error line tied to one resource.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredEvent {
    pub id: i64,
    pub ts: DateTime<Utc>,
    pub resource_id: String,
    pub kind: String,
    pub severity: String,
    pub message: String,
}

pub(crate) const EVENTS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS sample_buckets (
    resource_id TEXT NOT NULL,
    metric      TEXT NOT NULL,
    bucket_ms   INTEGER NOT NULL,
    n           INTEGER NOT NULL,
    vmin        REAL NOT NULL,
    vmax        REAL NOT NULL,
    vsum        REAL NOT NULL,
    PRIMARY KEY (resource_id, metric, bucket_ms)
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    resource_id TEXT NOT NULL,
    kind        TEXT NOT NULL,
    severity    TEXT NOT NULL,
    message     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_res ON events(resource_id, ts);
CREATE INDEX IF NOT EXISTS events_ts ON events(ts);
CREATE UNIQUE INDEX IF NOT EXISTS events_dedupe ON events(resource_id, ts, kind, message);

CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
    message,
    resource_id,
    tokenize = 'unicode61'
);
"#;

/// Start of the 5-minute window that contains `ts_ms`. The window is still open.
pub fn bucket_start(ts_ms: i64) -> i64 {
    ts_ms.div_euclid(BUCKET_MS) * BUCKET_MS
}

pub(crate) fn ensure_history(conn: &Connection) -> crate::Result<()> {
    conn.execute_batch(EVENTS_DDL)?;
    Ok(())
}

/// Fold one raw sample into its bucket, and record a restart when a restart counter rises.
pub(crate) fn observe_sample(
    conn: &Connection,
    resource_id: &str,
    metric: &str,
    ts_ms: i64,
    value: f64,
) -> crate::Result<()> {
    if is_restart_metric(metric) {
        let prev: Option<f64> = conn
            .query_row(
                "SELECT value FROM latest WHERE resource_id = ?1 AND metric = ?2",
                params![resource_id, metric],
                |r| r.get(0),
            )
            .optional()?;
        if prev.is_some_and(|p| value > p) {
            insert_event(
                conn,
                ts_ms,
                resource_id,
                "restart",
                "warning",
                &format!("restart count {value}"),
            )?;
        }
    }
    let bucket = bucket_start(ts_ms);
    conn.execute(
        "INSERT INTO sample_buckets (resource_id, metric, bucket_ms, n, vmin, vmax, vsum)
         VALUES (?1, ?2, ?3, 1, ?4, ?4, ?4)
         ON CONFLICT(resource_id, metric, bucket_ms) DO UPDATE SET
            n = n + 1,
            vmin = MIN(vmin, excluded.vmin),
            vmax = MAX(vmax, excluded.vmax),
            vsum = vsum + excluded.vsum",
        params![resource_id, metric, bucket, value],
    )?;
    Ok(())
}

pub(crate) fn note_changes(conn: &Connection, changes: &[Change]) -> crate::Result<()> {
    for change in changes {
        let ms = change.ts.timestamp_millis();
        if change.kind == ChangeKind::Appeared {
            if let Some(health) = change
                .after
                .as_ref()
                .and_then(|v| v.get("health"))
                .and_then(|v| v.as_str())
            {
                note_health(conn, ms, &change.resource_id, health)?;
            }
        } else if change.field.as_deref() == Some("health") {
            if let Some(health) = json_str(change.after.as_ref()) {
                note_health(conn, ms, &change.resource_id, health)?;
            }
        } else if change.field.as_deref() == Some("attrs.restart_count") {
            let before = json_f64(change.before.as_ref()).unwrap_or(0.0);
            let after = json_f64(change.after.as_ref()).unwrap_or(0.0);
            if after > before {
                insert_event(
                    conn,
                    ms,
                    &change.resource_id,
                    "restart",
                    "warning",
                    &format!("restart count {after}"),
                )?;
            }
        } else if change.field.as_deref() == Some("attrs.sub") {
            if let Some(sub) = json_str(change.after.as_ref()) {
                if sub.to_ascii_lowercase().contains("restart") {
                    insert_event(
                        conn,
                        ms,
                        &change.resource_id,
                        "restart",
                        "warning",
                        &format!("sub {sub}"),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn note_health(conn: &Connection, ms: i64, resource_id: &str, health: &str) -> crate::Result<()> {
    let severity = match health {
        "failed" => "critical",
        "degraded" => "warning",
        _ => return Ok(()),
    };
    insert_event(
        conn,
        ms,
        resource_id,
        "failure",
        severity,
        &format!("health {health}"),
    )
}

/// Error lines only. Callers do not pass info logs.
pub(crate) fn note_error_log(
    conn: &Connection,
    ts_ms: i64,
    resource_id: &str,
    message: &str,
) -> crate::Result<()> {
    if resource_id.is_empty() || message.is_empty() {
        return Ok(());
    }
    insert_event(conn, ts_ms, resource_id, "error", "warning", message)
}

pub(crate) fn store_pushed(conn: &Connection, event: &StoredEvent) -> crate::Result<()> {
    insert_event(
        conn,
        event.ts.timestamp_millis(),
        &event.resource_id,
        &event.kind,
        &event.severity,
        &event.message,
    )
}

fn insert_event(
    conn: &Connection,
    ts_ms: i64,
    resource_id: &str,
    kind: &str,
    severity: &str,
    message: &str,
) -> crate::Result<()> {
    let wrote = conn.execute(
        "INSERT OR IGNORE INTO events (ts, resource_id, kind, severity, message)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![ts_ms, resource_id, kind, severity, message],
    )?;
    if wrote == 1 {
        let id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO events_fts(rowid, message, resource_id) VALUES (?1, ?2, ?3)",
            params![id, message, resource_id],
        )?;
    }
    Ok(())
}

fn is_restart_metric(metric: &str) -> bool {
    metric == "k8s.restarts" || metric.ends_with(".restarts")
}

fn json_str(v: Option<&serde_json::Value>) -> Option<&str> {
    v.and_then(|v| v.as_str())
}

fn json_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    v.as_f64()
        .or_else(|| v.as_i64().map(|n| n as f64))
        .or_else(|| v.as_u64().map(|n| n as f64))
}

/// Quote each word so a search string cannot change the FTS query shape.
pub fn fts_query(text: &str) -> Option<String> {
    let parts: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| format!("\"{t}\""))
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

impl Store {
    /// Closed buckets with `bucket_ms` strictly after `after_ms` and before `open_ms`.
    pub fn closed_buckets_since(
        &self,
        after_ms: i64,
        open_ms: i64,
        limit: usize,
    ) -> crate::Result<Vec<MetricBucket>> {
        let limit = limit.clamp(1, 5_000) as i64;
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT resource_id, metric, bucket_ms, n, vmin, vmax, vsum
             FROM sample_buckets
             WHERE bucket_ms > ?1 AND bucket_ms < ?2
             ORDER BY bucket_ms LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![after_ms, open_ms, limit], row_bucket)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Replace a bucket when the incoming row has at least as many samples.
    pub fn upsert_buckets(&self, buckets: &[MetricBucket]) -> crate::Result<()> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "INSERT INTO sample_buckets (resource_id, metric, bucket_ms, n, vmin, vmax, vsum)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(resource_id, metric, bucket_ms) DO UPDATE SET
                n = excluded.n, vmin = excluded.vmin, vmax = excluded.vmax, vsum = excluded.vsum
             WHERE excluded.n >= sample_buckets.n",
        )?;
        for b in buckets {
            if b.n == 0 || !b.min.is_finite() || !b.max.is_finite() || !b.sum.is_finite() {
                continue;
            }
            stmt.execute(params![
                b.resource_id,
                b.metric,
                b.bucket_ms,
                b.n as i64,
                b.min,
                b.max,
                b.sum
            ])?;
        }
        Ok(())
    }

    pub fn events_after(&self, after_id: i64, limit: usize) -> crate::Result<Vec<StoredEvent>> {
        let limit = limit.clamp(1, 5_000) as i64;
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, ts, resource_id, kind, severity, message FROM events
             WHERE id > ?1 ORDER BY id LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after_id, limit], row_event)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn insert_events(&self, events: &[StoredEvent]) -> crate::Result<i64> {
        let mut max_id = 0i64;
        let conn = self.conn();
        for event in events {
            max_id = max_id.max(event.id);
            note_error_or_stored(&conn, event)?;
        }
        Ok(max_id)
    }

    /// Points from raw samples when they cover `from`, otherwise from buckets.
    /// `(step_ms, points)`. Bucket queries widen `step_ms` to at least 5 minutes.
    pub fn history_points(
        &self,
        id: &str,
        metric: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        step_ms: i64,
    ) -> crate::Result<(i64, Vec<pulseblade_core::Point>)> {
        let from_ms = from.timestamp_millis();
        let oldest: Option<i64> = self
            .conn()
            .query_row(
                "SELECT MIN(ts) FROM samples WHERE resource_id = ?1 AND metric = ?2",
                params![id, metric],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let raw_covers = oldest.is_some_and(|min| min <= from_ms);
        if raw_covers {
            let points = self.series(
                id,
                metric,
                from,
                to,
                step_ms,
                pulseblade_core::Aggregation::Avg,
            )?;
            return Ok((step_ms, points));
        }
        let step = step_ms.max(BUCKET_MS);
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT (bucket_ms / ?1) * ?1 AS b, SUM(vsum) / SUM(n)
             FROM sample_buckets
             WHERE resource_id = ?2 AND metric = ?3 AND bucket_ms BETWEEN ?4 AND ?5
             GROUP BY b ORDER BY b",
        )?;
        let rows = stmt.query_map(
            params![step, id, metric, from_ms, to.timestamp_millis()],
            |r| {
                Ok(pulseblade_core::Point {
                    ts: crate::from_ms(r.get(0)?),
                    value: r.get(1)?,
                })
            },
        )?;
        Ok((step, rows.collect::<rusqlite::Result<_>>()?))
    }

    /// Newest matches first. `before_id` is 0 for the first page.
    pub fn search_events(
        &self,
        text: &str,
        since_ms: i64,
        resource: Option<&str>,
        before_id: i64,
        limit: usize,
    ) -> crate::Result<Vec<StoredEvent>> {
        let Some(query) = fts_query(text) else {
            return Ok(Vec::new());
        };
        let resource = resource.unwrap_or("");
        let limit = limit.clamp(1, 50) as i64;
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.ts, e.resource_id, e.kind, e.severity, e.message
             FROM events_fts
             JOIN events e ON e.id = events_fts.rowid
             WHERE events_fts MATCH ?1
               AND e.ts >= ?2
               AND (?3 = '' OR e.resource_id = ?3)
               AND (?4 = 0 OR e.id < ?4)
             ORDER BY e.ts DESC, e.id DESC
             LIMIT ?5",
        )?;
        let rows = stmt.query_map(
            params![query, since_ms, resource, before_id, limit],
            row_event,
        )?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Error events for `ids`, newest first. Used when the logs table has no copy.
    pub fn error_events(
        &self,
        ids: &[String],
        limit: usize,
    ) -> crate::Result<Vec<pulseblade_core::LogLine>> {
        if ids.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT ts, resource_id, message FROM events
             WHERE kind = 'error' AND resource_id IN ({placeholders})
             ORDER BY ts DESC LIMIT {limit}"
        ))?;
        let rows = stmt.query_map(rusqlite::params_from_iter(ids.iter()), |r| {
            Ok(pulseblade_core::LogLine {
                ts: from_ms(r.get(0)?),
                resource_id: r.get(1)?,
                message: r.get(2)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

fn note_error_or_stored(conn: &Connection, event: &StoredEvent) -> crate::Result<()> {
    store_pushed(conn, event)
}

fn row_bucket(r: &rusqlite::Row<'_>) -> rusqlite::Result<MetricBucket> {
    Ok(MetricBucket {
        resource_id: r.get(0)?,
        metric: r.get(1)?,
        bucket_ms: r.get(2)?,
        n: r.get::<_, i64>(3)? as u64,
        min: r.get(4)?,
        max: r.get(5)?,
        sum: r.get(6)?,
    })
}

fn row_event(r: &rusqlite::Row<'_>) -> rusqlite::Result<StoredEvent> {
    Ok(StoredEvent {
        id: r.get(0)?,
        ts: from_ms(r.get(1)?),
        resource_id: r.get(2)?,
        kind: r.get(3)?,
        severity: r.get(4)?,
        message: r.get(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};

    #[test]
    fn buckets_survive_raw_prune_and_search_finds_errors() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        let old = now - Duration::days(3);
        store
            .apply(
                "host",
                old,
                &Observation {
                    resources: vec![
                        Resource::new("host:h", ResourceKind::Host, "h").health(Health::Failed)
                    ],
                    samples: vec![Sample::new("host:h", "cpu.used_pct", 10.0)],
                    logs: vec![pulseblade_core::LogLine {
                        ts: old,
                        resource_id: "host:h".into(),
                        message: "disk controller timeout".into(),
                    }],
                },
            )
            .unwrap();
        store
            .apply(
                "host",
                now,
                &Observation {
                    resources: vec![
                        Resource::new("host:h", ResourceKind::Host, "h").health(Health::Ok)
                    ],
                    samples: vec![Sample::new("host:h", "cpu.used_pct", 20.0)],
                    logs: vec![],
                },
            )
            .unwrap();
        let (raw_gone, _) = store
            .prune(
                now - Duration::hours(1),
                now - Duration::days(30),
                now - Duration::days(90),
            )
            .unwrap();
        assert!(raw_gone >= 1);
        let (_, points) = store
            .history_points(
                "host:h",
                "cpu.used_pct",
                now - Duration::days(10),
                now,
                BUCKET_MS,
            )
            .unwrap();
        assert!(
            points.iter().any(|p| (p.value - 10.0).abs() < 0.01),
            "rolled-up sample missing: {points:?}"
        );
        let hits = store
            .search_events(
                "timeout",
                (now - Duration::days(90)).timestamp_millis(),
                None,
                0,
                10,
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].resource_id, "host:h");
        assert_eq!(hits[0].kind, "error");
        let failures = store
            .search_events("failed", 0, Some("host:h"), 0, 10)
            .unwrap();
        assert!(failures.iter().any(|h| h.kind == "failure"));
        assert!(store
            .search_events("zzzz-not-a-log", 0, None, 0, 10)
            .unwrap()
            .is_empty());

        store
            .prune(
                now - Duration::hours(1),
                now - Duration::days(1),
                now - Duration::days(1),
            )
            .unwrap();
        assert!(store
            .search_events("timeout", 0, None, 0, 10)
            .unwrap()
            .is_empty());
        let (_, recent) = store
            .history_points(
                "host:h",
                "cpu.used_pct",
                now - Duration::hours(1),
                now,
                1_000,
            )
            .unwrap();
        assert!(recent.iter().any(|p| (p.value - 20.0).abs() < 0.01));
    }

    #[test]
    fn restart_counter_is_one_event() {
        let store = Store::open_in_memory().unwrap();
        let t0 = Utc::now() - Duration::minutes(1);
        let unit = |n: u64| {
            Resource::new("pod:c:ns:api", ResourceKind::Pod, "api")
                .health(Health::Ok)
                .attr("restart_count", n)
        };
        store
            .apply(
                "k8s",
                t0,
                &Observation {
                    resources: vec![unit(1)],
                    samples: vec![Sample::new("pod:c:ns:api", "k8s.restarts", 1.0)],
                    logs: vec![],
                },
            )
            .unwrap();
        store
            .apply(
                "k8s",
                Utc::now(),
                &Observation {
                    resources: vec![unit(2)],
                    samples: vec![Sample::new("pod:c:ns:api", "k8s.restarts", 3.0)],
                    logs: vec![],
                },
            )
            .unwrap();
        let hits = store
            .search_events("restart", 0, Some("pod:c:ns:api"), 0, 10)
            .unwrap();
        assert!(hits.len() >= 2, "{hits:?}");
        assert!(hits.iter().all(|h| h.kind == "restart"));
    }
}
