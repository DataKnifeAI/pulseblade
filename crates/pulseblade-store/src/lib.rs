//! Embedded SQLite store for Pulseblade.
//!
//! Holds current resource state, the append-only change journal, checkpoints,
//! and metric samples. Collectors hand over complete [`Observation`]s; the store
//! diffs them against what it already knows to produce [`Change`]s.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use chrono::{DateTime, TimeZone, Utc};
mod actions;
mod history;
mod hub;
mod memory;
mod pilot;
mod sql;

pub use actions::{ActionTransition, NewAction};
pub use history::{MetricBucket, StoredEvent, BUCKET_MS, DEFAULT_HISTORY_DAYS, HUB_RAW_HOURS};
pub use hub::{IngestResult, NodeBatch, NodeCursor, NodePush, RouteJob, StampedSample};
pub use memory::{FindingDraft, FindingFilter};
pub use pilot::{NewPilotRun, PilotRunRow};

use pulseblade_core::{
    Aggregation, Change, ChangeKind, Checkpoint, CollectorRun, Health, Observation, Point,
    Resource, ResourceKind, Since, StoredResource,
};
use rusqlite::{params, Connection, Row};

use sql::{SqlExt, SqlVal};

const RESOURCE_COLS: &str = "SELECT id, kind, name, parent, health, labels, attrs, source, first_seen, last_seen, present FROM resources";
use serde_json::{json, Value};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("unknown checkpoint `{0}`")]
    UnknownCheckpoint(String),
    #[error("corrupt row: {0}")]
    Corrupt(String),
    #[error("unknown finding `{0}`")]
    UnknownFinding(String),
    #[error("unknown action `{0}`")]
    UnknownAction(String),
    #[error("{0}")]
    Invalid(String),
    /// A node sent a journal seq the hub has not caught up to yet.
    #[error("journal gap for node {node}: hub has seq {have}, next change is {next}")]
    Gap { node: String, have: i64, next: i64 },
    #[error("database: {0}")]
    Database(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS resources (
    id         TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    name       TEXT NOT NULL,
    parent     TEXT,
    health     TEXT NOT NULL,
    labels     TEXT NOT NULL,
    attrs      TEXT NOT NULL,
    source     TEXT NOT NULL,
    first_seen INTEGER NOT NULL,
    last_seen  INTEGER NOT NULL,
    present    INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS resources_source ON resources(source);
CREATE INDEX IF NOT EXISTS resources_parent ON resources(parent);

CREATE TABLE IF NOT EXISTS changes (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    resource_id TEXT NOT NULL,
    kind        TEXT NOT NULL,
    field       TEXT,
    before      TEXT,
    after       TEXT
);
CREATE INDEX IF NOT EXISTS changes_resource ON changes(resource_id, seq);
CREATE INDEX IF NOT EXISTS changes_ts ON changes(ts);

CREATE TABLE IF NOT EXISTS checkpoints (
    name TEXT PRIMARY KEY,
    seq  INTEGER NOT NULL,
    ts   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
    resource_id TEXT NOT NULL,
    metric      TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    value       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS samples_series ON samples(resource_id, metric, ts);

CREATE TABLE IF NOT EXISTS sample_count (
    id INTEGER PRIMARY KEY,
    n  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS latest (
    resource_id TEXT NOT NULL,
    metric      TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    value       REAL NOT NULL,
    PRIMARY KEY (resource_id, metric)
);

CREATE TABLE IF NOT EXISTS collector_runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source         TEXT NOT NULL,
    ts             INTEGER NOT NULL,
    duration_ms    INTEGER NOT NULL,
    ok             INTEGER NOT NULL,
    error          TEXT,
    resource_count INTEGER NOT NULL,
    sample_count   INTEGER NOT NULL,
    change_count   INTEGER NOT NULL,
    interval_secs  INTEGER
);
CREATE INDEX IF NOT EXISTS collector_runs_source ON collector_runs(source, id);

CREATE TABLE IF NOT EXISTS logs (
    ts          INTEGER NOT NULL,
    resource_id TEXT NOT NULL,
    message     TEXT NOT NULL,
    PRIMARY KEY (resource_id, ts, message)
);
CREATE INDEX IF NOT EXISTS logs_ts ON logs(ts);

CREATE TABLE IF NOT EXISTS notes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_id TEXT NOT NULL,
    author      TEXT NOT NULL,
    text        TEXT NOT NULL,
    created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS notes_resource ON notes(resource_id, created_at);

CREATE TABLE IF NOT EXISTS findings (
    id          TEXT PRIMARY KEY,
    dedupe_key  TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    rule        TEXT NOT NULL,
    severity    TEXT NOT NULL,
    status      TEXT NOT NULL,
    summary     TEXT NOT NULL,
    opened_at   INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    context     TEXT NOT NULL,
    logs        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS findings_status ON findings(status, opened_at);
CREATE INDEX IF NOT EXISTS findings_key ON findings(dedupe_key);
CREATE INDEX IF NOT EXISTS findings_resource ON findings(resource_id);

CREATE TABLE IF NOT EXISTS action_runs (
    id               TEXT PRIMARY KEY,
    action           TEXT NOT NULL,
    resource_id      TEXT NOT NULL,
    params           TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    tier             TEXT NOT NULL,
    status           TEXT NOT NULL,
    dry_run          INTEGER NOT NULL,
    output           TEXT,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    expires_at       INTEGER,
    finding_id       TEXT
);
CREATE INDEX IF NOT EXISTS action_runs_key ON action_runs(idempotency_key, status);
CREATE INDEX IF NOT EXISTS action_runs_resource ON action_runs(resource_id, created_at);

CREATE TABLE IF NOT EXISTS audit (
    seq       INTEGER PRIMARY KEY,
    ts        INTEGER NOT NULL,
    run_id    TEXT NOT NULL,
    event     TEXT NOT NULL,
    payload   TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pilot_runs (
    id         TEXT PRIMARY KEY,
    ts         INTEGER NOT NULL,
    trigger    TEXT NOT NULL,
    tier       TEXT NOT NULL,
    endpoint   TEXT NOT NULL,
    tool_calls INTEGER NOT NULL,
    tokens     INTEGER NOT NULL,
    conclusion TEXT NOT NULL,
    proposed   TEXT NOT NULL,
    status     TEXT NOT NULL,
    detail     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS node_cursors (
    node_id       TEXT PRIMARY KEY,
    seq           INTEGER NOT NULL,
    sample_ms     INTEGER NOT NULL,
    last_push_ms  INTEGER NOT NULL DEFAULT 0,
    interval_secs INTEGER NOT NULL DEFAULT 15,
    shutting_down INTEGER NOT NULL DEFAULT 0,
    bucket_ms     INTEGER NOT NULL DEFAULT 0,
    event_id      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS route_rpc (
    id         TEXT PRIMARY KEY,
    node_id    TEXT NOT NULL,
    op         TEXT NOT NULL,
    body       TEXT NOT NULL,
    status     TEXT NOT NULL,
    result     TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS route_rpc_node ON route_rpc(node_id, status);

CREATE TABLE IF NOT EXISTS route_plans (
    plan_id     TEXT PRIMARY KEY,
    node_id     TEXT NOT NULL,
    finding_id  TEXT,
    resource_id TEXT
);
CREATE INDEX IF NOT EXISTS route_plans_resource ON route_plans(resource_id);

CREATE TABLE IF NOT EXISTS route_results (
    id      TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    body    TEXT NOT NULL
);
"#;

/// Collector runs kept per source; older rows are pruned on insert.
const RUNS_KEPT_PER_SOURCE: i64 = 240;

/// Filter for [`Store::resources`]. Empty fields match everything.
#[derive(Debug, Clone, Default)]
pub struct ResourceFilter {
    pub kind: Option<ResourceKind>,
    /// Case-insensitive substring of id or name.
    pub query: Option<String>,
    /// Every label must match exactly.
    pub labels: BTreeMap<String, String>,
    pub unhealthy_only: bool,
    pub include_absent: bool,
}

impl ResourceFilter {
    fn matches(&self, r: &StoredResource) -> bool {
        if !self.include_absent && !r.present {
            return false;
        }
        if self.kind.is_some_and(|k| k != r.resource.kind) {
            return false;
        }
        if self.unhealthy_only && !r.resource.health.is_unhealthy() {
            return false;
        }
        if let Some(q) = &self.query {
            let q = q.to_lowercase();
            if !r.resource.id.to_lowercase().contains(&q)
                && !r.resource.name.to_lowercase().contains(&q)
            {
                return false;
            }
        }
        self.labels
            .iter()
            .all(|(k, v)| r.resource.labels.get(k) == Some(v))
    }
}

/// Scope for change queries. Empty fields match everything.
#[derive(Debug, Clone, Default)]
pub struct ChangeFilter {
    pub kind: Option<ResourceKind>,
    pub resource_prefix: Option<String>,
}

/// The latest value of one metric.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Latest {
    pub ts: DateTime<Utc>,
    pub value: f64,
}

/// The most recent run of one collector, with failure context.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectorSummary {
    pub last: CollectorRun,
    /// Most recent successful run among the retained rows.
    pub last_success: Option<DateTime<Utc>>,
    /// Failed runs since the last success.
    pub consecutive_failures: usize,
}

/// Row counts for self-observability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreStats {
    pub samples: u64,
    pub changes: u64,
    pub checkpoints: u64,
}

pub struct Store {
    backend: sql::Backend,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        Self::init(Connection::open(path)?, Some(path.to_path_buf()))
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?, None)
    }

    fn init(conn: Connection, path: Option<PathBuf>) -> Result<Self> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        history::ensure_history(&conn)?;
        ensure_cursor_columns(&conn)?;
        {
            let mut sql = sql::SqliteSql { conn: &conn };
            ensure_sample_count(&mut sql)?;
            ensure_later_columns(&mut sql)?;
        }
        Ok(Self {
            backend: sql::Backend::Sqlite {
                conn: Mutex::new(conn),
                path,
            },
        })
    }
}

/// Databases created before later cursor columns still have the old table.
fn ensure_cursor_columns(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(node_cursors)")?;
    let names: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<std::result::Result<_, _>>()?;
    for (name, ddl) in [
        ("last_push_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("interval_secs", "INTEGER NOT NULL DEFAULT 15"),
        ("shutting_down", "INTEGER NOT NULL DEFAULT 0"),
        ("bucket_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("event_id", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        if !names.iter().any(|have| have == name) {
            conn.execute(
                &format!("ALTER TABLE node_cursors ADD COLUMN {name} {ddl}"),
                [],
            )?;
        }
    }
    Ok(())
}

/// Columns added after the first schema. `CREATE TABLE IF NOT EXISTS` does not
/// alter a table that already exists.
pub(crate) fn ensure_later_columns(sql: &mut dyn sql::Sql) -> Result<()> {
    ensure_column(sql, "action_runs", "finding_id", "TEXT")?;
    ensure_column(sql, "route_plans", "finding_id", "TEXT")?;
    ensure_column(sql, "route_plans", "resource_id", "TEXT")?;
    sql.execute(
        "CREATE INDEX IF NOT EXISTS route_plans_resource ON route_plans(resource_id)",
        &[],
    )?;
    Ok(())
}

fn ensure_column(sql: &mut dyn sql::Sql, table: &str, column: &str, ddl: &str) -> Result<()> {
    let present = if sql.postgres() {
        sql.query_one(
            "SELECT COUNT(*) FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = ?1 AND column_name = ?2",
            &[SqlVal::text(table), SqlVal::text(column)],
            |row| row.int(0),
        )? > 0
    } else {
        let names = sql.query(&format!("PRAGMA table_info({table})"), &[], |row| {
            row.text(1)
        })?;
        names.iter().any(|name| name == column)
    };
    if !present {
        sql.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {ddl}"),
            &[],
        )?;
    }
    Ok(())
}

/// `sample_count` is one row (`id = 1`). `COUNT(*)` runs only when that row
/// is missing: store open, or a later read after the row was removed.
fn ensure_sample_count(sql: &mut dyn sql::Sql) -> Result<()> {
    let present = sql.query_opt("SELECT id FROM sample_count WHERE id = 1", &[], |row| {
        row.int(0)
    })?;
    if present.is_some() {
        return Ok(());
    }
    let n = sql.query_one("SELECT COUNT(*) FROM samples", &[], |row| row.int(0))?;
    sql.execute(
        "INSERT INTO sample_count (id, n) VALUES (1, ?1) ON CONFLICT(id) DO NOTHING",
        &[SqlVal::Int(n)],
    )?;
    Ok(())
}

fn add_sample_count(sql: &mut dyn sql::Sql, delta: i64) -> Result<()> {
    if delta == 0 {
        return Ok(());
    }
    ensure_sample_count(sql)?;
    sql.execute(
        "UPDATE sample_count SET n = n + ?1 WHERE id = 1",
        &[SqlVal::Int(delta)],
    )?;
    Ok(())
}

fn sample_count(sql: &mut dyn sql::Sql) -> Result<u64> {
    ensure_sample_count(sql)?;
    let n = sql.query_one("SELECT n FROM sample_count WHERE id = 1", &[], |row| {
        row.int(0)
    })?;
    Ok(n.max(0) as u64)
}

impl Store {
    /// Database file, or `None` for an in-memory or Postgres store.
    pub fn path(&self) -> Option<&Path> {
        match &self.backend {
            sql::Backend::Sqlite { path, .. } => path.as_deref(),
            sql::Backend::Postgres { .. } => None,
        }
    }

    /// Bytes on disk for the SQLite file and its WAL, or the Postgres database.
    pub fn size_bytes(&self) -> Option<u64> {
        if matches!(&self.backend, sql::Backend::Postgres { .. }) {
            return self
                .with_sql(|sql| {
                    sql.query_one("SELECT pg_database_size(current_database())", &[], |row| {
                        row.int(0)
                    })
                })
                .ok()
                .map(|n| n as u64);
        }
        let path = self.path()?;
        let main = std::fs::metadata(path).ok()?.len();
        let mut wal = path.to_path_buf().into_os_string();
        wal.push("-wal");
        Some(main + std::fs::metadata(wal).map(|m| m.len()).unwrap_or(0))
    }

    pub fn stats(&self) -> Result<StoreStats> {
        self.with_sql(|sql| {
            let count = |sql: &mut dyn sql::Sql, table: &str| -> Result<u64> {
                Ok(
                    sql.query_one(&format!("SELECT COUNT(*) FROM {table}"), &[], |row| {
                        row.int(0)
                    })? as u64,
                )
            };
            Ok(StoreStats {
                samples: sample_count(sql)?,
                changes: count(sql, "changes")?,
                checkpoints: count(sql, "checkpoints")?,
            })
        })
    }

    /// Persist one collector pass and prune old runs of the same source.
    pub fn record_run(&self, run: &CollectorRun) -> Result<()> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO collector_runs
                (source, ts, duration_ms, ok, error, resource_count, sample_count, change_count, interval_secs)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                run.source,
                run.ts.timestamp_millis(),
                run.duration_ms as i64,
                run.ok,
                run.error,
                run.resource_count as i64,
                run.sample_count as i64,
                run.change_count as i64,
                run.interval_secs.map(|s| s as i64),
            ],
        )?;
        conn.execute(
            "DELETE FROM collector_runs WHERE source = ?1 AND id <= (
                SELECT id FROM collector_runs WHERE source = ?1
                ORDER BY id DESC LIMIT 1 OFFSET ?2)",
            params![run.source, RUNS_KEPT_PER_SOURCE],
        )?;
        Ok(())
    }

    /// Latest run of every collector that has ever recorded one, by source.
    pub fn collector_summaries(&self) -> Result<Vec<CollectorSummary>> {
        self.with_sql(|sql| {
            sql.query(
                "SELECT r.source, r.ts, r.duration_ms, r.ok, r.error, r.resource_count,
                        r.sample_count, r.change_count, r.interval_secs,
                        (SELECT MAX(ts) FROM collector_runs s WHERE s.source = r.source AND s.ok = 1),
                        (SELECT COUNT(*) FROM collector_runs f WHERE f.source = r.source AND f.ok = 0
                            AND f.id > COALESCE((SELECT MAX(id) FROM collector_runs g
                                                 WHERE g.source = r.source AND g.ok = 1), 0))
                 FROM collector_runs r
                 WHERE r.id IN (SELECT MAX(id) FROM collector_runs GROUP BY source)
                 ORDER BY r.source",
                &[],
                |row| {
                    Ok(CollectorSummary {
                        last: CollectorRun {
                            source: row.text(0)?,
                            ts: from_ms(row.int(1)?),
                            duration_ms: row.int(2)? as u64,
                            ok: row.int(3)? != 0,
                            error: row.opt_text(4)?,
                            resource_count: row.int(5)? as usize,
                            sample_count: row.int(6)? as usize,
                            change_count: row.int(7)? as usize,
                            interval_secs: row.opt_int(8)?.map(|s| s as u64),
                        },
                        last_success: row.opt_int(9)?.map(from_ms),
                        consecutive_failures: row.int(10)? as usize,
                    })
                },
            )
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.sqlite_conn()
    }

    /// Record one collector pass and return the resulting changes.
    pub fn apply(&self, source: &str, ts: DateTime<Utc>, obs: &Observation) -> Result<Vec<Change>> {
        let ms = ts.timestamp_millis();
        let mut conn = self.conn();
        let tx = conn.transaction()?;

        let mut known: HashMap<String, StoredResource> = HashMap::new();
        {
            let mut stmt = tx.prepare("SELECT * FROM resources WHERE source = ?1")?;
            let rows = stmt.query_map([source], row_to_resource)?;
            for r in rows {
                let r = r??;
                known.insert(r.resource.id.clone(), r);
            }
        }

        let change = |resource_id: &str, kind, field, before, after| Change {
            seq: 0,
            ts,
            resource_id: resource_id.to_string(),
            kind,
            field,
            before,
            after,
        };
        let mut changes: Vec<Change> = Vec::new();
        for res in &obs.resources {
            match known.remove(&res.id) {
                Some(prev) if prev.present => {
                    for (field, before, after) in diff(&prev.resource, res) {
                        changes.push(change(
                            &res.id,
                            ChangeKind::Changed,
                            Some(field),
                            before,
                            after,
                        ));
                    }
                }
                _ => changes.push(change(
                    &res.id,
                    ChangeKind::Appeared,
                    None,
                    None,
                    Some(summary(res)),
                )),
            }
            tx.execute(
                "INSERT INTO resources (id, kind, name, parent, health, labels, attrs, source, first_seen, last_seen, present)
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

        for (id, prev) in known {
            if prev.present {
                tx.execute("UPDATE resources SET present = 0 WHERE id = ?1", [&id])?;
                changes.push(change(
                    &id,
                    ChangeKind::Disappeared,
                    None,
                    Some(summary(&prev.resource)),
                    None,
                ));
            }
        }

        for c in &mut changes {
            tx.execute(
                "INSERT INTO changes (ts, resource_id, kind, field, before, after) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    ms,
                    c.resource_id,
                    c.kind.as_str(),
                    c.field,
                    c.before.as_ref().map(Value::to_string),
                    c.after.as_ref().map(Value::to_string),
                ],
            )?;
            c.seq = tx.last_insert_rowid();
        }
        history::note_changes(&tx, &changes)?;

        {
            let mut session = sql::SqliteSql { conn: &tx };
            ensure_sample_count(&mut session)?;
        }
        let mut inserted_samples = 0i64;
        {
            let mut ins = tx.prepare_cached(
                "INSERT INTO samples (resource_id, metric, ts, value) VALUES (?1, ?2, ?3, ?4)",
            )?;
            let mut latest = tx.prepare_cached(
                "INSERT INTO latest (resource_id, metric, ts, value) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(resource_id, metric) DO UPDATE SET ts = excluded.ts, value = excluded.value",
            )?;
            for s in obs.samples.iter().filter(|s| s.value.is_finite()) {
                history::observe_sample(&tx, &s.resource_id, &s.metric, ms, s.value)?;
                ins.execute(params![s.resource_id, s.metric, ms, s.value])?;
                latest.execute(params![s.resource_id, s.metric, ms, s.value])?;
                inserted_samples += 1;
            }
        }
        {
            let mut session = sql::SqliteSql { conn: &tx };
            add_sample_count(&mut session, inserted_samples)?;
        }

        {
            let mut ins = tx.prepare_cached(
                "INSERT OR IGNORE INTO logs (ts, resource_id, message) VALUES (?1, ?2, ?3)",
            )?;
            for line in &obs.logs {
                let message = clip_log(&line.message);
                if line.resource_id.is_empty() || message.is_empty() {
                    continue;
                }
                let line_ms = line.ts.timestamp_millis();
                ins.execute(params![line_ms, line.resource_id, message])?;
                history::note_error_log(&tx, line_ms, &line.resource_id, &message)?;
            }
        }

        tx.commit()?;
        Ok(changes)
    }

    /// Highest sequence number in the change journal (0 when empty).
    pub fn current_seq(&self) -> Result<i64> {
        self.with_sql(|sql| {
            sql.query_one("SELECT COALESCE(MAX(seq), 0) FROM changes", &[], |row| {
                row.int(0)
            })
        })
    }

    pub fn resources(&self, filter: &ResourceFilter) -> Result<Vec<StoredResource>> {
        self.with_sql(|sql| {
            let rows = sql.query(&format!("{RESOURCE_COLS} ORDER BY id"), &[], map_resource)?;
            Ok(rows.into_iter().filter(|row| filter.matches(row)).collect())
        })
    }

    pub fn resource(&self, id: &str) -> Result<Option<StoredResource>> {
        self.with_sql(|sql| {
            sql.query_opt(
                &format!("{RESOURCE_COLS} WHERE id = ?1"),
                &[SqlVal::text(id)],
                map_resource,
            )
        })
    }

    pub fn children(&self, id: &str) -> Result<Vec<StoredResource>> {
        self.with_sql(|sql| {
            sql.query(
                &format!("{RESOURCE_COLS} WHERE parent = ?1 AND present = 1 ORDER BY id"),
                &[SqlVal::text(id)],
                map_resource,
            )
        })
    }

    /// Latest value of every metric for one resource.
    pub fn latest(&self, id: &str) -> Result<BTreeMap<String, Latest>> {
        self.with_sql(|sql| {
            let rows = sql.query(
                "SELECT metric, ts, value FROM latest WHERE resource_id = ?1",
                &[SqlVal::text(id)],
                |row| {
                    Ok((
                        row.text(0)?,
                        Latest {
                            ts: from_ms(row.int(1)?),
                            value: row.float(2)?,
                        },
                    ))
                },
            )?;
            Ok(rows.into_iter().collect())
        })
    }

    /// Latest value of every metric for every resource.
    pub fn latest_all(&self) -> Result<HashMap<String, BTreeMap<String, f64>>> {
        self.with_sql(|sql| {
            let rows = sql.query(
                "SELECT resource_id, metric, value FROM latest",
                &[],
                |row| Ok((row.text(0)?, row.text(1)?, row.float(2)?)),
            )?;
            let mut out: HashMap<String, BTreeMap<String, f64>> = HashMap::new();
            for (id, metric, value) in rows {
                out.entry(id).or_default().insert(metric, value);
            }
            Ok(out)
        })
    }

    /// Downsampled series: one point per `step` bucket in `[from, to]`.
    pub fn series(
        &self,
        id: &str,
        metric: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        step_ms: i64,
        agg: Aggregation,
    ) -> Result<Vec<Point>> {
        let step = step_ms.max(1);
        let select = match agg {
            Aggregation::Avg => "AVG(value)",
            Aggregation::Min => "MIN(value)",
            Aggregation::Max => "MAX(value)",
            // SQLite returns bare columns from the row that produced MAX().
            Aggregation::Last => "value, MAX(ts)",
        };
        self.with_sql(|sql| {
            let statement = if sql.postgres() && matches!(agg, Aggregation::Last) {
                "SELECT bucket, value FROM (
                    SELECT DISTINCT ON ((ts / ?1) * ?1) (ts / ?1) * ?1 AS bucket, value
                    FROM samples
                    WHERE resource_id = ?2 AND metric = ?3 AND ts BETWEEN ?4 AND ?5
                    ORDER BY (ts / ?1) * ?1, ts DESC
                 ) s ORDER BY bucket"
                    .to_string()
            } else {
                format!(
                    "SELECT (ts / ?1) * ?1 AS bucket, {select} FROM samples
                     WHERE resource_id = ?2 AND metric = ?3 AND ts BETWEEN ?4 AND ?5
                     GROUP BY bucket ORDER BY bucket"
                )
            };
            sql.query(
                &statement,
                &[
                    SqlVal::Int(step),
                    SqlVal::text(id),
                    SqlVal::text(metric),
                    SqlVal::Int(from.timestamp_millis()),
                    SqlVal::Int(to.timestamp_millis()),
                ],
                |row| {
                    Ok(Point {
                        ts: from_ms(row.int(0)?),
                        value: row.float(1)?,
                    })
                },
            )
        })
    }

    pub fn create_checkpoint(&self, name: &str, ts: DateTime<Utc>) -> Result<Checkpoint> {
        let seq = self.current_seq()?;
        self.with_sql(|sql| {
            sql.execute(
                "INSERT INTO checkpoints (name, seq, ts) VALUES (?1, ?2, ?3)
                 ON CONFLICT(name) DO UPDATE SET seq = excluded.seq, ts = excluded.ts",
                &[
                    SqlVal::text(name),
                    SqlVal::Int(seq),
                    SqlVal::Int(ts.timestamp_millis()),
                ],
            )?;
            Ok(Checkpoint {
                name: name.to_string(),
                seq,
                ts,
            })
        })
    }

    pub fn checkpoints(&self) -> Result<Vec<Checkpoint>> {
        self.with_sql(|sql| {
            sql.query(
                "SELECT name, seq, ts FROM checkpoints ORDER BY seq DESC",
                &[],
                |row| {
                    Ok(Checkpoint {
                        name: row.text(0)?,
                        seq: row.int(1)?,
                        ts: from_ms(row.int(2)?),
                    })
                },
            )
        })
    }

    /// Translate a `since` expression into the journal sequence to read after.
    pub fn resolve_since(&self, since: &Since) -> Result<i64> {
        match since {
            Since::Seq(n) => Ok(*n),
            Since::Checkpoint(name) => self.with_sql(|sql| {
                sql.query_opt(
                    "SELECT seq FROM checkpoints WHERE name = ?1",
                    &[SqlVal::text(name)],
                    |row| row.int(0),
                )?
                .ok_or_else(|| StoreError::UnknownCheckpoint(name.clone()))
            }),
            Since::Time(t) => self.with_sql(|sql| {
                sql.query_one(
                    "SELECT COALESCE(MAX(seq), 0) FROM changes WHERE ts < ?1",
                    &[SqlVal::Int(t.timestamp_millis())],
                    |row| row.int(0),
                )
            }),
        }
    }

    /// Changes with `seq > after`, oldest first, plus the total that matched.
    pub fn changes_after(
        &self,
        after: i64,
        filter: &ChangeFilter,
        limit: usize,
    ) -> Result<(Vec<Change>, usize)> {
        let kind = filter.kind.map(|k| k.as_str().to_string());
        let prefix = filter.resource_prefix.as_ref().map(|p| format!("{p}%"));
        let where_clause = "c.seq > ?1
             AND (CAST(?2 AS TEXT) IS NULL OR r.kind = ?2)
             AND (CAST(?3 AS TEXT) IS NULL OR c.resource_id LIKE ?3)";
        let args = [
            SqlVal::Int(after),
            SqlVal::opt_text(kind),
            SqlVal::opt_text(prefix),
        ];
        self.with_sql(|sql| {
            let total = sql.query_one(
                &format!(
                    "SELECT COUNT(*) FROM changes c LEFT JOIN resources r ON r.id = c.resource_id WHERE {where_clause}"
                ),
                &args,
                |row| row.int(0),
            )?;
            let mut listed = args.to_vec();
            listed.push(SqlVal::Int(limit as i64));
            let changes = sql.query(
                &format!(
                    "SELECT c.seq, c.ts, c.resource_id, c.kind, c.field, c.before, c.after
                     FROM changes c LEFT JOIN resources r ON r.id = c.resource_id
                     WHERE {where_clause} ORDER BY c.seq LIMIT ?4"
                ),
                &listed,
                map_change,
            )?;
            Ok((changes, total as usize))
        })
    }

    /// Most recent changes touching any of `ids`, newest first.
    pub fn recent_changes(&self, ids: &[String], limit: usize) -> Result<Vec<Change>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        let args: Vec<SqlVal> = ids.iter().cloned().map(SqlVal::text).collect();
        self.with_sql(|sql| {
            sql.query(
                &format!(
                    "SELECT seq, ts, resource_id, kind, field, before, after FROM changes
                     WHERE resource_id IN ({placeholders}) ORDER BY seq DESC LIMIT {limit}"
                ),
                &args,
                map_change,
            )
        })
    }

    /// Drop raw samples older than `samples_before`, journal rows older than
    /// `changes_before`, and buckets plus events older than `history_before`.
    /// Buckets are not deleted with the raw samples they summarize.
    pub fn prune(
        &self,
        samples_before: DateTime<Utc>,
        changes_before: DateTime<Utc>,
        history_before: DateTime<Utc>,
    ) -> Result<(usize, usize)> {
        let sample_ms = samples_before.timestamp_millis();
        let history_ms = history_before.timestamp_millis();
        let change_ms = changes_before.timestamp_millis();
        self.transaction(|sql| {
            ensure_sample_count(sql)?;
            let s = sql.execute(
                "DELETE FROM samples WHERE ts < ?1",
                &[SqlVal::Int(sample_ms)],
            )?;
            add_sample_count(sql, -(s as i64))?;
            sql.execute("DELETE FROM logs WHERE ts < ?1", &[SqlVal::Int(sample_ms)])?;
            if !sql.postgres() {
                sql.execute(
                    "DELETE FROM events_fts WHERE rowid IN (SELECT id FROM events WHERE ts < ?1)",
                    &[SqlVal::Int(history_ms)],
                )?;
            }
            sql.execute(
                "DELETE FROM events WHERE ts < ?1",
                &[SqlVal::Int(history_ms)],
            )?;
            sql.execute(
                "DELETE FROM sample_buckets WHERE bucket_ms < ?1",
                &[SqlVal::Int(history_ms)],
            )?;
            let c = sql.execute(
                "DELETE FROM changes WHERE ts < ?1",
                &[SqlVal::Int(change_ms)],
            )?;
            sql.execute(
                "DELETE FROM resources WHERE present = 0 AND last_seen < ?1",
                &[SqlVal::Int(change_ms)],
            )?;
            Ok((s, c))
        })
    }
}

fn diff(prev: &Resource, next: &Resource) -> Vec<(String, Option<Value>, Option<Value>)> {
    let mut out = Vec::new();
    if prev.name != next.name {
        out.push((
            "name".into(),
            Some(json!(prev.name)),
            Some(json!(next.name)),
        ));
    }
    if prev.parent != next.parent {
        out.push((
            "parent".into(),
            json_opt(&prev.parent),
            json_opt(&next.parent),
        ));
    }
    if prev.health != next.health {
        out.push((
            "health".into(),
            Some(json!(prev.health)),
            Some(json!(next.health)),
        ));
    }
    diff_map("labels", &prev.labels, &next.labels, &mut out, |v| json!(v));
    diff_map("attrs", &prev.attrs, &next.attrs, &mut out, Value::clone);
    out
}

fn diff_map<V: PartialEq>(
    prefix: &str,
    prev: &BTreeMap<String, V>,
    next: &BTreeMap<String, V>,
    out: &mut Vec<(String, Option<Value>, Option<Value>)>,
    to_json: impl Fn(&V) -> Value,
) {
    for (k, v) in next {
        match prev.get(k) {
            Some(p) if p == v => {}
            p => out.push((format!("{prefix}.{k}"), p.map(&to_json), Some(to_json(v)))),
        }
    }
    for (k, p) in prev {
        if !next.contains_key(k) {
            out.push((format!("{prefix}.{k}"), Some(to_json(p)), None));
        }
    }
}

fn json_opt(v: &Option<String>) -> Option<Value> {
    v.as_ref().map(|s| json!(s))
}

/// Keep log lines short enough to store and return without a bulk dump.
fn clip_log(message: &str) -> String {
    const MAX: usize = 400;
    let flat: String = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= MAX {
        return flat;
    }
    let mut out: String = flat.chars().take(MAX - 1).collect();
    out.push('…');
    out
}

fn summary(r: &Resource) -> Value {
    json!({ "kind": r.kind, "name": r.name, "health": r.health })
}

fn from_ms(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms).single().unwrap_or_default()
}

fn parse_json<T: serde::de::DeserializeOwned>(s: Option<String>) -> Result<Option<T>> {
    s.map(|s| serde_json::from_str(&s))
        .transpose()
        .map_err(Into::into)
}

fn map_resource(row: &dyn sql::SqlRow) -> Result<StoredResource> {
    let kind = row.text(1)?;
    let health = row.text(4)?;
    let labels = row.text(5)?;
    let attrs = row.text(6)?;
    Ok(StoredResource {
        resource: Resource {
            id: row.text(0)?,
            kind: kind.parse().map_err(StoreError::Corrupt)?,
            name: row.text(2)?,
            parent: row.opt_text(3)?,
            health: health.parse().map_err(StoreError::Corrupt)?,
            labels: serde_json::from_str(&labels)?,
            attrs: serde_json::from_str(&attrs)?,
        },
        source: row.text(7)?,
        first_seen: from_ms(row.int(8)?),
        last_seen: from_ms(row.int(9)?),
        present: row.int(10)? != 0,
    })
}

fn map_change(row: &dyn sql::SqlRow) -> Result<Change> {
    let kind = row.text(3)?;
    Ok(Change {
        seq: row.int(0)?,
        ts: from_ms(row.int(1)?),
        resource_id: row.text(2)?,
        kind: kind.parse().map_err(StoreError::Corrupt)?,
        field: row.opt_text(4)?,
        before: parse_json(row.opt_text(5)?)?,
        after: parse_json(row.opt_text(6)?)?,
    })
}

fn row_to_resource(row: &Row<'_>) -> rusqlite::Result<Result<StoredResource>> {
    let kind: String = row.get("kind")?;
    let health: String = row.get("health")?;
    let labels: String = row.get("labels")?;
    let attrs: String = row.get("attrs")?;
    let id: String = row.get("id")?;
    let name: String = row.get("name")?;
    let parent: Option<String> = row.get("parent")?;
    let source: String = row.get("source")?;
    let first_seen: i64 = row.get("first_seen")?;
    let last_seen: i64 = row.get("last_seen")?;
    let present: bool = row.get("present")?;
    Ok((|| {
        Ok(StoredResource {
            resource: Resource {
                id,
                kind: kind.parse().map_err(StoreError::Corrupt)?,
                name,
                parent,
                health: health.parse::<Health>().map_err(StoreError::Corrupt)?,
                labels: serde_json::from_str(&labels)?,
                attrs: serde_json::from_str(&attrs)?,
            },
            source,
            first_seen: from_ms(first_seen),
            last_seen: from_ms(last_seen),
            present,
        })
    })())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use pulseblade_core::Sample;

    fn unit(name: &str, active: &str, health: Health) -> Resource {
        Resource::new(format!("unit:h:{name}"), ResourceKind::Service, name)
            .parent("host:h")
            .health(health)
            .attr("active", active)
    }

    fn obs(resources: Vec<Resource>) -> Observation {
        Observation {
            resources,
            samples: vec![],
            logs: vec![],
        }
    }

    #[test]
    fn diffing_produces_appeared_changed_disappeared() {
        let store = Store::open_in_memory().unwrap();
        let t0 = Utc::now();

        let c = store
            .apply(
                "systemd",
                t0,
                &obs(vec![
                    unit("a.service", "active", Health::Ok),
                    unit("b.service", "active", Health::Ok),
                ]),
            )
            .unwrap();
        assert_eq!(c.len(), 2);
        assert!(c.iter().all(|c| c.kind == ChangeKind::Appeared));

        let unchanged = store
            .apply(
                "systemd",
                t0,
                &obs(vec![
                    unit("a.service", "active", Health::Ok),
                    unit("b.service", "active", Health::Ok),
                ]),
            )
            .unwrap();
        assert!(unchanged.is_empty());

        let c = store
            .apply(
                "systemd",
                t0,
                &obs(vec![unit("a.service", "failed", Health::Failed)]),
            )
            .unwrap();
        let fields: Vec<_> = c.iter().map(|c| (c.kind, c.field.clone())).collect();
        assert!(fields.contains(&(ChangeKind::Changed, Some("health".into()))));
        assert!(fields.contains(&(ChangeKind::Changed, Some("attrs.active".into()))));
        assert!(fields.contains(&(ChangeKind::Disappeared, None)));

        let b = store.resource("unit:h:b.service").unwrap().unwrap();
        assert!(!b.present);
        assert!(store
            .resources(&ResourceFilter::default())
            .unwrap()
            .iter()
            .all(|r| r.resource.id != "unit:h:b.service"));

        let c = store
            .apply(
                "systemd",
                t0,
                &obs(vec![
                    unit("a.service", "failed", Health::Failed),
                    unit("b.service", "active", Health::Ok),
                ]),
            )
            .unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].kind, ChangeKind::Appeared);
    }

    #[test]
    fn sources_do_not_clobber_each_other() {
        let store = Store::open_in_memory().unwrap();
        let t0 = Utc::now();
        store
            .apply(
                "systemd",
                t0,
                &obs(vec![unit("a.service", "active", Health::Ok)]),
            )
            .unwrap();
        let c = store
            .apply(
                "host",
                t0,
                &obs(vec![Resource::new("host:h", ResourceKind::Host, "h")]),
            )
            .unwrap();
        assert_eq!(c.len(), 1);
        assert!(store.resource("unit:h:a.service").unwrap().unwrap().present);
    }

    #[test]
    fn checkpoints_and_since() {
        let store = Store::open_in_memory().unwrap();
        let t0 = Utc::now() - Duration::minutes(10);
        store
            .apply(
                "systemd",
                t0,
                &obs(vec![unit("a.service", "active", Health::Ok)]),
            )
            .unwrap();
        let cp = store.create_checkpoint("before", Utc::now()).unwrap();
        assert_eq!(cp.seq, 1);

        let t1 = Utc::now();
        store
            .apply(
                "systemd",
                t1,
                &obs(vec![unit("a.service", "failed", Health::Failed)]),
            )
            .unwrap();

        let after = store
            .resolve_since(&Since::Checkpoint("before".into()))
            .unwrap();
        let (changes, total) = store
            .changes_after(after, &ChangeFilter::default(), 100)
            .unwrap();
        assert_eq!(total, 2);
        assert!(changes.iter().all(|c| c.seq > cp.seq));

        let by_time = store
            .resolve_since(&Since::Time(t1 - Duration::seconds(1)))
            .unwrap();
        assert_eq!(by_time, 1);

        let (scoped, _) = store
            .changes_after(
                0,
                &ChangeFilter {
                    kind: Some(ResourceKind::Host),
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        assert!(scoped.is_empty());

        assert!(matches!(
            store.resolve_since(&Since::Checkpoint("nope".into())),
            Err(StoreError::UnknownCheckpoint(_))
        ));
    }

    #[test]
    fn collector_runs_summarize_and_prune() {
        let store = Store::open_in_memory().unwrap();
        let t0 = Utc::now() - Duration::minutes(10);
        let run = |source: &str, i: i64, ok: bool| CollectorRun {
            source: source.into(),
            ts: t0 + Duration::seconds(i),
            duration_ms: 5,
            ok,
            error: (!ok).then(|| "boom".to_string()),
            resource_count: 3,
            sample_count: 7,
            change_count: 0,
            interval_secs: Some(15),
        };
        for i in 0..RUNS_KEPT_PER_SOURCE + 10 {
            store.record_run(&run("host", i, true)).unwrap();
        }
        store.record_run(&run("systemd", 0, true)).unwrap();
        store.record_run(&run("systemd", 1, false)).unwrap();
        store.record_run(&run("systemd", 2, false)).unwrap();

        let kept: i64 = store
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM collector_runs WHERE source = 'host'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(kept, RUNS_KEPT_PER_SOURCE);

        let s = store.collector_summaries().unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].last.source, "host");
        assert!(s[0].last.ok);
        assert_eq!(s[0].consecutive_failures, 0);
        assert_eq!(s[1].last.error.as_deref(), Some("boom"));
        assert_eq!(s[1].consecutive_failures, 2);
        assert_eq!(s[1].last_success, Some(from_ms(t0.timestamp_millis())));
    }

    #[test]
    fn series_downsamples() {
        let store = Store::open_in_memory().unwrap();
        let base = Utc.timestamp_millis_opt(1_800_000_000_000).unwrap();
        for i in 0..6 {
            store
                .apply(
                    "host",
                    base + Duration::seconds(i * 10),
                    &Observation {
                        resources: vec![Resource::new("host:h", ResourceKind::Host, "h")],
                        samples: vec![Sample::new("host:h", "cpu.used_pct", i as f64)],
                        logs: vec![],
                    },
                )
                .unwrap();
        }
        let to = base + Duration::minutes(1);
        let avg = store
            .series("host:h", "cpu.used_pct", base, to, 30_000, Aggregation::Avg)
            .unwrap();
        assert_eq!(avg.len(), 2);
        assert_eq!(avg[0].value, 1.0);
        assert_eq!(avg[1].value, 4.0);
        let last = store
            .series(
                "host:h",
                "cpu.used_pct",
                base,
                to,
                30_000,
                Aggregation::Last,
            )
            .unwrap();
        assert_eq!(last[1].value, 5.0);
        assert_eq!(store.latest("host:h").unwrap()["cpu.used_pct"].value, 5.0);
    }

    #[test]
    fn sample_counter_tracks_apply_prune_and_reseed() {
        let path = std::env::temp_dir().join(format!(
            "pulseblade-sample-count-{}-{}.db",
            std::process::id(),
            Utc::now().timestamp_millis()
        ));
        let _ = std::fs::remove_file(&path);
        let old = Utc.timestamp_millis_opt(1_700_000_000_000).unwrap();
        let new = old + Duration::hours(2);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.stats().unwrap().samples, 0);

        store
            .apply(
                "host",
                old,
                &Observation {
                    resources: vec![Resource::new("host:h", ResourceKind::Host, "h")],
                    samples: vec![
                        Sample::new("host:h", "cpu.used_pct", 1.0),
                        Sample::new("host:h", "mem.used_pct", 2.0),
                        Sample::new("host:h", "load", f64::NAN),
                    ],
                    logs: vec![],
                },
            )
            .unwrap();
        store
            .apply(
                "host",
                new,
                &Observation {
                    resources: vec![Resource::new("host:h", ResourceKind::Host, "h")],
                    samples: vec![
                        Sample::new("host:h", "cpu.used_pct", 3.0),
                        Sample::new("host:h", "load", 4.0),
                    ],
                    logs: vec![],
                },
            )
            .unwrap();
        let stats = store.stats().unwrap();
        assert_eq!(stats.samples, 4);
        assert!(stats.changes >= 1);
        assert_eq!(stats.checkpoints, 0);
        store.create_checkpoint("mark", new).unwrap();
        assert_eq!(store.stats().unwrap().checkpoints, 1);

        let (removed, _) = store
            .prune(
                old + Duration::hours(1),
                new + Duration::days(1),
                new + Duration::days(1),
            )
            .unwrap();
        assert_eq!(removed, 2);
        assert_eq!(store.stats().unwrap().samples, 2);
        let live: i64 = store
            .conn()
            .query_row("SELECT COUNT(*) FROM samples", [], |row| row.get(0))
            .unwrap();
        assert_eq!(store.stats().unwrap().samples, live as u64);

        store
            .conn()
            .execute("DELETE FROM sample_count", [])
            .unwrap();
        assert_eq!(store.stats().unwrap().samples, 2);
        drop(store);

        let store = Store::open(&path).unwrap();
        store
            .conn()
            .execute("DELETE FROM sample_count", [])
            .unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.stats().unwrap().samples, 2);
        drop(store);
        for suffix in ["", "-wal", "-shm"] {
            let mut sidecar = path.clone().into_os_string();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(sidecar);
        }
    }
}
