//! Embedded SQLite store for Pulseblade.
//!
//! Holds current resource state, the append-only change journal, checkpoints,
//! and metric samples. Collectors hand over complete [`Observation`]s; the store
//! diffs them against what it already knows to produce [`Change`]s.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use chrono::{DateTime, TimeZone, Utc};
use pulseblade_core::{
    Aggregation, Change, ChangeKind, Checkpoint, Health, Observation, Point, Resource,
    ResourceKind, Since, StoredResource,
};
use rusqlite::{params, Connection, OptionalExtension, Row};
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

CREATE TABLE IF NOT EXISTS latest (
    resource_id TEXT NOT NULL,
    metric      TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    value       REAL NOT NULL,
    PRIMARY KEY (resource_id, metric)
);
"#;

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

pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::init(Connection::open(path)?)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
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

        {
            let mut ins = tx.prepare_cached(
                "INSERT INTO samples (resource_id, metric, ts, value) VALUES (?1, ?2, ?3, ?4)",
            )?;
            let mut latest = tx.prepare_cached(
                "INSERT INTO latest (resource_id, metric, ts, value) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(resource_id, metric) DO UPDATE SET ts = excluded.ts, value = excluded.value",
            )?;
            for s in obs.samples.iter().filter(|s| s.value.is_finite()) {
                ins.execute(params![s.resource_id, s.metric, ms, s.value])?;
                latest.execute(params![s.resource_id, s.metric, ms, s.value])?;
            }
        }

        tx.commit()?;
        Ok(changes)
    }

    /// Highest sequence number in the change journal (0 when empty).
    pub fn current_seq(&self) -> Result<i64> {
        Ok(self
            .conn()
            .query_row("SELECT COALESCE(MAX(seq), 0) FROM changes", [], |r| {
                r.get(0)
            })?)
    }

    pub fn resources(&self, filter: &ResourceFilter) -> Result<Vec<StoredResource>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT * FROM resources ORDER BY id")?;
        let rows = stmt.query_map([], row_to_resource)?;
        let mut out = Vec::new();
        for r in rows {
            let r = r??;
            if filter.matches(&r) {
                out.push(r);
            }
        }
        Ok(out)
    }

    pub fn resource(&self, id: &str) -> Result<Option<StoredResource>> {
        self.conn()
            .query_row(
                "SELECT * FROM resources WHERE id = ?1",
                [id],
                row_to_resource,
            )
            .optional()?
            .transpose()
    }

    pub fn children(&self, id: &str) -> Result<Vec<StoredResource>> {
        let conn = self.conn();
        let mut stmt =
            conn.prepare("SELECT * FROM resources WHERE parent = ?1 AND present = 1 ORDER BY id")?;
        let rows = stmt.query_map([id], row_to_resource)?;
        rows.map(|r| r?).collect()
    }

    /// Latest value of every metric for one resource.
    pub fn latest(&self, id: &str) -> Result<BTreeMap<String, Latest>> {
        let conn = self.conn();
        let mut stmt =
            conn.prepare("SELECT metric, ts, value FROM latest WHERE resource_id = ?1")?;
        let rows = stmt.query_map([id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                Latest {
                    ts: from_ms(r.get(1)?),
                    value: r.get(2)?,
                },
            ))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Latest value of every metric for every resource.
    pub fn latest_all(&self) -> Result<HashMap<String, BTreeMap<String, f64>>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT resource_id, metric, value FROM latest")?;
        let mut out: HashMap<String, BTreeMap<String, f64>> = HashMap::new();
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get(2)?))
        })?;
        for row in rows {
            let (id, metric, value) = row?;
            out.entry(id).or_default().insert(metric, value);
        }
        Ok(out)
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
        let sql = format!(
            "SELECT (ts / ?1) * ?1 AS bucket, {select} FROM samples
             WHERE resource_id = ?2 AND metric = ?3 AND ts BETWEEN ?4 AND ?5
             GROUP BY bucket ORDER BY bucket"
        );
        let conn = self.conn();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params![
                step,
                id,
                metric,
                from.timestamp_millis(),
                to.timestamp_millis()
            ],
            |r| {
                Ok(Point {
                    ts: from_ms(r.get(0)?),
                    value: r.get(1)?,
                })
            },
        )?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn create_checkpoint(&self, name: &str, ts: DateTime<Utc>) -> Result<Checkpoint> {
        let seq = self.current_seq()?;
        self.conn().execute(
            "INSERT INTO checkpoints (name, seq, ts) VALUES (?1, ?2, ?3)
             ON CONFLICT(name) DO UPDATE SET seq = excluded.seq, ts = excluded.ts",
            params![name, seq, ts.timestamp_millis()],
        )?;
        Ok(Checkpoint {
            name: name.to_string(),
            seq,
            ts,
        })
    }

    pub fn checkpoints(&self) -> Result<Vec<Checkpoint>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT name, seq, ts FROM checkpoints ORDER BY seq DESC")?;
        let rows = stmt.query_map([], |r| {
            Ok(Checkpoint {
                name: r.get(0)?,
                seq: r.get(1)?,
                ts: from_ms(r.get(2)?),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Translate a `since` expression into the journal sequence to read after.
    pub fn resolve_since(&self, since: &Since) -> Result<i64> {
        let conn = self.conn();
        match since {
            Since::Seq(n) => Ok(*n),
            Since::Checkpoint(name) => conn
                .query_row("SELECT seq FROM checkpoints WHERE name = ?1", [name], |r| {
                    r.get(0)
                })
                .optional()?
                .ok_or_else(|| StoreError::UnknownCheckpoint(name.clone())),
            Since::Time(t) => Ok(conn.query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM changes WHERE ts < ?1",
                [t.timestamp_millis()],
                |r| r.get(0),
            )?),
        }
    }

    /// Changes with `seq > after`, oldest first, plus the total that matched.
    pub fn changes_after(
        &self,
        after: i64,
        filter: &ChangeFilter,
        limit: usize,
    ) -> Result<(Vec<Change>, usize)> {
        let kind = filter.kind.map(ResourceKind::as_str);
        let prefix = filter.resource_prefix.as_ref().map(|p| format!("{p}%"));
        let where_clause = "c.seq > ?1
             AND (?2 IS NULL OR r.kind = ?2)
             AND (?3 IS NULL OR c.resource_id LIKE ?3)";
        let conn = self.conn();
        let total: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM changes c LEFT JOIN resources r ON r.id = c.resource_id WHERE {where_clause}"
            ),
            params![after, kind, prefix],
            |r| r.get(0),
        )?;
        let mut stmt = conn.prepare(&format!(
            "SELECT c.seq, c.ts, c.resource_id, c.kind, c.field, c.before, c.after
             FROM changes c LEFT JOIN resources r ON r.id = c.resource_id
             WHERE {where_clause} ORDER BY c.seq LIMIT ?4"
        ))?;
        let rows = stmt.query_map(params![after, kind, prefix, limit as i64], row_to_change)?;
        let changes = rows.map(|r| r?).collect::<Result<Vec<_>>>()?;
        Ok((changes, total as usize))
    }

    /// Most recent changes touching any of `ids`, newest first.
    pub fn recent_changes(&self, ids: &[String], limit: usize) -> Result<Vec<Change>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT seq, ts, resource_id, kind, field, before, after FROM changes
             WHERE resource_id IN ({placeholders}) ORDER BY seq DESC LIMIT {limit}"
        ))?;
        let rows = stmt.query_map(rusqlite::params_from_iter(ids), row_to_change)?;
        rows.map(|r| r?).collect()
    }

    /// Drop samples older than `samples_before` and changes older than `changes_before`.
    pub fn prune(
        &self,
        samples_before: DateTime<Utc>,
        changes_before: DateTime<Utc>,
    ) -> Result<(usize, usize)> {
        let conn = self.conn();
        let s = conn.execute(
            "DELETE FROM samples WHERE ts < ?1",
            [samples_before.timestamp_millis()],
        )?;
        let c = conn.execute(
            "DELETE FROM changes WHERE ts < ?1",
            [changes_before.timestamp_millis()],
        )?;
        conn.execute(
            "DELETE FROM resources WHERE present = 0 AND last_seen < ?1",
            [changes_before.timestamp_millis()],
        )?;
        Ok((s, c))
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

fn row_to_change(row: &Row<'_>) -> rusqlite::Result<Result<Change>> {
    let seq: i64 = row.get(0)?;
    let ts: i64 = row.get(1)?;
    let resource_id: String = row.get(2)?;
    let kind: String = row.get(3)?;
    let field: Option<String> = row.get(4)?;
    let before: Option<String> = row.get(5)?;
    let after: Option<String> = row.get(6)?;
    Ok((|| {
        Ok(Change {
            seq,
            ts: from_ms(ts),
            resource_id,
            kind: kind.parse().map_err(StoreError::Corrupt)?,
            field,
            before: parse_json(before)?,
            after: parse_json(after)?,
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
}
