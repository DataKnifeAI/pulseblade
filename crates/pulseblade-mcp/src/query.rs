//! Agent-shaped queries over the store, shared by the MCP tools and `pulseblade ctl`.
//!
//! Responses are bounded, ordered by relevance, and always carry `as_of_seq` so
//! the caller can use it as its next `changes_since` cursor.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use pulseblade_core::{
    parse_duration, Aggregation, Change, ChangeKind, Checkpoint, Health, Point, ResourceKind,
    Since, StoredResource,
};
use pulseblade_store::{ChangeFilter, ResourceFilter, Store, StoreError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const DEFAULT_SNAPSHOT_LIMIT: usize = 50;
const MAX_SNAPSHOT_LIMIT: usize = 500;
const DEFAULT_CHANGES_LIMIT: usize = 100;
const MAX_CHANGES_LIMIT: usize = 1000;
const MAX_POINTS: i64 = 300;
const DEFAULT_EXPORT_LINES: usize = 2000;
const MAX_EXPORT_LINES: usize = 10_000;

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, QueryError>;

// ---------------------------------------------------------------------------
// state_snapshot

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct SnapshotParams {
    /// Only resources of this kind.
    #[serde(default)]
    pub kind: Option<ResourceKind>,
    /// Case-insensitive substring of resource id or name.
    #[serde(default)]
    pub query: Option<String>,
    /// Every label must match exactly, e.g. {"criticality": "high"}.
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
    /// Only failed or degraded resources.
    #[serde(default)]
    pub unhealthy_only: Option<bool>,
    /// Include attributes for every resource (larger response).
    #[serde(default)]
    pub verbose: Option<bool>,
    /// Maximum resources returned (default 50, max 500). Unhealthy resources sort first.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ResourceBrief {
    pub id: String,
    pub kind: ResourceKind,
    pub name: String,
    pub health: Health,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Latest metric values, rounded to two decimals.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metrics: BTreeMap<String, f64>,
    /// Attributes, present when `verbose` or when the resource is unhealthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attrs: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct KindCounts {
    pub total: usize,
    pub failed: usize,
    pub degraded: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Snapshot {
    /// Journal position this snapshot reflects; pass to `changes_since` later.
    pub as_of_seq: i64,
    pub generated_at: DateTime<Utc>,
    /// Counts across all matching resources, keyed by kind.
    pub counts: BTreeMap<String, KindCounts>,
    /// Matching resources: unhealthy first, then by kind and id.
    pub resources: Vec<ResourceBrief>,
    pub total: usize,
    pub truncated: bool,
}

pub fn snapshot(store: &Store, p: SnapshotParams) -> Result<Snapshot> {
    let as_of_seq = store.current_seq()?;
    let filter = ResourceFilter {
        kind: p.kind,
        query: p.query,
        labels: p.labels.unwrap_or_default(),
        unhealthy_only: p.unhealthy_only.unwrap_or(false),
        include_absent: false,
    };
    let mut all = store.resources(&filter)?;
    let latest = store.latest_all()?;
    let verbose = p.verbose.unwrap_or(false);
    let limit = p
        .limit
        .unwrap_or(DEFAULT_SNAPSHOT_LIMIT)
        .clamp(1, MAX_SNAPSHOT_LIMIT);

    let mut counts: BTreeMap<String, KindCounts> = BTreeMap::new();
    for r in &all {
        let c = counts.entry(r.resource.kind.to_string()).or_default();
        c.total += 1;
        match r.resource.health {
            Health::Failed => c.failed += 1,
            Health::Degraded => c.degraded += 1,
            _ => {}
        }
    }

    all.sort_by(|a, b| {
        (a.resource.health, a.resource.kind, &a.resource.id).cmp(&(
            b.resource.health,
            b.resource.kind,
            &b.resource.id,
        ))
    });
    let total = all.len();
    let resources = all
        .into_iter()
        .take(limit)
        .map(|r| brief(r, &latest, verbose))
        .collect();

    Ok(Snapshot {
        as_of_seq,
        generated_at: Utc::now(),
        counts,
        resources,
        total,
        truncated: total > limit,
    })
}

fn brief(
    r: StoredResource,
    latest: &std::collections::HashMap<String, BTreeMap<String, f64>>,
    verbose: bool,
) -> ResourceBrief {
    let res = r.resource;
    let metrics = latest
        .get(&res.id)
        .map(|m| m.iter().map(|(k, v)| (k.clone(), round2(*v))).collect())
        .unwrap_or_default();
    let attrs = (verbose || res.health.is_unhealthy()).then_some(res.attrs);
    ResourceBrief {
        id: res.id,
        kind: res.kind,
        name: res.name,
        health: res.health,
        labels: res.labels,
        metrics,
        attrs,
    }
}

// ---------------------------------------------------------------------------
// resource_explain

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExplainParams {
    /// Resource id, e.g. `unit:web1:sshd.service`. Use `state_snapshot` to discover ids.
    pub id: String,
    /// How many recent changes to include for the resource and its parent (default 20).
    #[serde(default)]
    pub changes_limit: Option<usize>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct MetricValue {
    pub value: f64,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ChildrenSummary {
    pub total: usize,
    pub by_kind: BTreeMap<String, KindCounts>,
    /// Children that are failed or degraded, with attributes.
    pub unhealthy: Vec<ResourceBrief>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Explanation {
    pub as_of_seq: i64,
    pub resource: StoredResource,
    /// Latest value of every metric; query history with `metrics_query`.
    pub metrics: BTreeMap<String, MetricValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ResourceBrief>,
    pub children: ChildrenSummary,
    /// Recent changes on this resource and its parent, newest first: causal context.
    pub recent_changes: Vec<Change>,
}

pub fn explain(store: &Store, p: ExplainParams) -> Result<Explanation> {
    let as_of_seq = store.current_seq()?;
    let Some(resource) = store.resource(&p.id)? else {
        return Err(not_found_with_suggestions(store, &p.id)?);
    };
    let latest_all = store.latest_all()?;
    let metrics = store
        .latest(&p.id)?
        .into_iter()
        .map(|(k, l)| {
            (
                k,
                MetricValue {
                    value: round2(l.value),
                    ts: l.ts,
                },
            )
        })
        .collect();

    let parent = match &resource.resource.parent {
        Some(pid) => store.resource(pid)?.map(|r| brief(r, &latest_all, false)),
        None => None,
    };

    let children = store.children(&p.id)?;
    let mut by_kind: BTreeMap<String, KindCounts> = BTreeMap::new();
    for c in &children {
        let k = by_kind.entry(c.resource.kind.to_string()).or_default();
        k.total += 1;
        match c.resource.health {
            Health::Failed => k.failed += 1,
            Health::Degraded => k.degraded += 1,
            _ => {}
        }
    }
    let total = children.len();
    let unhealthy = children
        .into_iter()
        .filter(|c| c.resource.health.is_unhealthy())
        .map(|c| brief(c, &latest_all, true))
        .collect();

    let mut ids = vec![p.id.clone()];
    ids.extend(resource.resource.parent.clone());
    let recent_changes = store.recent_changes(&ids, p.changes_limit.unwrap_or(20).min(200))?;

    Ok(Explanation {
        as_of_seq,
        resource,
        metrics,
        parent,
        children: ChildrenSummary {
            total,
            by_kind,
            unhealthy,
        },
        recent_changes,
    })
}

fn not_found_with_suggestions(store: &Store, id: &str) -> Result<QueryError> {
    let needle = id.rsplit(':').next().unwrap_or(id).to_string();
    let candidates: Vec<String> = store
        .resources(&ResourceFilter {
            query: Some(needle),
            ..Default::default()
        })?
        .into_iter()
        .take(10)
        .map(|r| r.resource.id)
        .collect();
    let hint = if candidates.is_empty() {
        "use state_snapshot to list resource ids".to_string()
    } else {
        format!("did you mean: {}", candidates.join(", "))
    };
    Ok(QueryError::NotFound(format!("no resource `{id}`; {hint}")))
}

// ---------------------------------------------------------------------------
// metrics_query

#[derive(Debug, Deserialize, JsonSchema)]
pub struct MetricsParams {
    /// Resource id.
    pub id: String,
    /// Metric name, e.g. `cpu.used_pct`. `resource_explain` lists a resource's metrics.
    pub metric: String,
    /// Look-back window such as `15m`, `1h`, `24h` (default `1h`).
    #[serde(default)]
    pub range: Option<String>,
    /// Bucket width such as `30s` or `5m`. Widened automatically to keep at most 300 points.
    #[serde(default)]
    pub step: Option<String>,
    /// Aggregation within each bucket (default `avg`).
    #[serde(default)]
    pub agg: Option<Aggregation>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SeriesStats {
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    pub last: f64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct Series {
    pub id: String,
    pub metric: String,
    pub agg: Aggregation,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub step_secs: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<SeriesStats>,
    pub points: Vec<Point>,
}

pub fn metrics(store: &Store, p: MetricsParams) -> Result<Series> {
    let range = parse_duration(p.range.as_deref().unwrap_or("1h"))
        .map_err(|e| QueryError::Invalid(e.to_string()))?;
    if range <= Duration::zero() {
        return Err(QueryError::Invalid("range must be positive".into()));
    }
    let min_step_ms = (range.num_milliseconds() / MAX_POINTS).max(1000);
    let step_ms = match p.step.as_deref() {
        Some(s) => parse_duration(s)
            .map_err(|e| QueryError::Invalid(e.to_string()))?
            .num_milliseconds()
            .max(min_step_ms),
        None => min_step_ms,
    };

    let known = store.latest(&p.id)?;
    if !known.contains_key(&p.metric) {
        if store.resource(&p.id)?.is_none() {
            return Err(not_found_with_suggestions(store, &p.id)?);
        }
        let names: Vec<_> = known.keys().cloned().collect();
        return Err(QueryError::NotFound(format!(
            "resource `{}` has no metric `{}`; available: {}",
            p.id,
            p.metric,
            if names.is_empty() {
                "(none)".to_string()
            } else {
                names.join(", ")
            }
        )));
    }

    let to = Utc::now();
    let from = to - range;
    let agg = p.agg.unwrap_or_default();
    let points: Vec<Point> = store
        .series(&p.id, &p.metric, from, to, step_ms, agg)?
        .into_iter()
        .map(|pt| Point {
            ts: pt.ts,
            value: round2(pt.value),
        })
        .collect();
    let stats = (!points.is_empty()).then(|| {
        let vals = points.iter().map(|p| p.value);
        SeriesStats {
            min: vals.clone().fold(f64::INFINITY, f64::min),
            max: vals.clone().fold(f64::NEG_INFINITY, f64::max),
            avg: round2(vals.sum::<f64>() / points.len() as f64),
            last: points[points.len() - 1].value,
        }
    });

    Ok(Series {
        id: p.id,
        metric: p.metric,
        agg,
        from,
        to,
        step_secs: step_ms / 1000,
        stats,
        points,
    })
}

// ---------------------------------------------------------------------------
// checkpoint_create

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CheckpointParams {
    /// Name for the current journal position, e.g. `before-deploy`. Reusing a name moves it.
    pub name: String,
}

pub fn checkpoint(store: &Store, p: CheckpointParams) -> Result<Checkpoint> {
    let name = p.name.trim();
    if name.is_empty() || name.parse::<i64>().is_ok() || name.starts_with("seq:") {
        return Err(QueryError::Invalid(
            "checkpoint name must be non-empty and not look like a sequence number".into(),
        ));
    }
    if parse_duration(name).is_ok() {
        return Err(QueryError::Invalid(
            "checkpoint name must not look like a duration (e.g. `15m`)".into(),
        ));
    }
    Ok(store.create_checkpoint(name, Utc::now())?)
}

// ---------------------------------------------------------------------------
// changes_since

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ChangesParams {
    /// Checkpoint name, `seq:<n>` (or a bare integer), RFC 3339 timestamp, or relative duration like `15m`.
    pub since: String,
    /// Only changes on resources of this kind.
    #[serde(default)]
    pub kind: Option<ResourceKind>,
    /// Only changes whose resource id starts with this prefix, e.g. `unit:web1:`.
    #[serde(default)]
    pub resource_prefix: Option<String>,
    /// Maximum changes returned (default 100, max 1000), oldest first.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct ChangeCounts {
    pub appeared: usize,
    pub changed: usize,
    pub disappeared: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ChangeSet {
    /// Changes returned are strictly after this sequence number.
    pub from_seq: i64,
    /// Current end of the journal.
    pub as_of_seq: i64,
    /// Pass as `since` to continue: the next page if truncated, otherwise future changes.
    pub next_cursor: String,
    pub total: usize,
    pub truncated: bool,
    /// Counts over the returned changes, by change kind.
    pub counts: ChangeCounts,
    pub changes: Vec<Change>,
}

pub fn changes(store: &Store, p: ChangesParams) -> Result<ChangeSet> {
    let since = Since::parse(&p.since, Utc::now());
    let from_seq = match store.resolve_since(&since) {
        Err(StoreError::UnknownCheckpoint(name)) => {
            let known: Vec<_> = store.checkpoints()?.into_iter().map(|c| c.name).collect();
            return Err(QueryError::NotFound(format!(
                "unknown checkpoint `{name}`; known: {}",
                if known.is_empty() {
                    "(none; create one with checkpoint_create)".to_string()
                } else {
                    known.join(", ")
                }
            )));
        }
        other => other?,
    };
    let as_of_seq = store.current_seq()?;
    let limit = p
        .limit
        .unwrap_or(DEFAULT_CHANGES_LIMIT)
        .clamp(1, MAX_CHANGES_LIMIT);
    let filter = ChangeFilter {
        kind: p.kind,
        resource_prefix: p.resource_prefix,
    };
    let (changes, total) = store.changes_after(from_seq, &filter, limit)?;
    let truncated = total > changes.len();
    let next = if truncated {
        changes.last().map_or(from_seq, |c| c.seq)
    } else {
        as_of_seq
    };
    let mut counts = ChangeCounts::default();
    for c in &changes {
        match c.kind {
            ChangeKind::Appeared => counts.appeared += 1,
            ChangeKind::Changed => counts.changed += 1,
            ChangeKind::Disappeared => counts.disappeared += 1,
        }
    }
    Ok(ChangeSet {
        from_seq,
        as_of_seq,
        next_cursor: format!("seq:{next}"),
        total,
        truncated,
        counts,
        changes,
    })
}

// ---------------------------------------------------------------------------
// export_bulk

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct ExportParams {
    /// Include changes since this point (same forms as `changes_since`). Omit for resources only.
    #[serde(default)]
    pub since: Option<String>,
    /// Only resources and changes of this kind.
    #[serde(default)]
    pub kind: Option<ResourceKind>,
    /// Maximum JSONL lines (default 2000, max 10000).
    #[serde(default)]
    pub max_lines: Option<usize>,
}

/// JSONL: one `resource` line per present resource (with latest metrics), then one
/// `change` line per change, then a final `meta` line.
pub fn export(store: &Store, p: ExportParams) -> Result<String> {
    let max = p
        .max_lines
        .unwrap_or(DEFAULT_EXPORT_LINES)
        .clamp(1, MAX_EXPORT_LINES);
    let as_of_seq = store.current_seq()?;
    let latest = store.latest_all()?;
    let mut lines = Vec::new();
    let mut truncated = false;

    let resources = store.resources(&ResourceFilter {
        kind: p.kind,
        ..Default::default()
    })?;
    for r in resources {
        if lines.len() >= max {
            truncated = true;
            break;
        }
        let metrics = latest.get(&r.resource.id).cloned().unwrap_or_default();
        lines.push(json!({ "type": "resource", "resource": r, "metrics": metrics }).to_string());
    }

    if let Some(since) = &p.since {
        let from_seq = store.resolve_since(&Since::parse(since, Utc::now()))?;
        let room = max.saturating_sub(lines.len());
        let (changes, total) = store.changes_after(
            from_seq,
            &ChangeFilter {
                kind: p.kind,
                resource_prefix: None,
            },
            room,
        )?;
        truncated |= total > changes.len();
        for c in changes {
            lines.push(json!({ "type": "change", "change": c }).to_string());
        }
    }

    lines.push(
        json!({ "type": "meta", "as_of_seq": as_of_seq, "truncated": truncated }).to_string(),
    );
    Ok(lines.join("\n"))
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulseblade_core::{Observation, Resource, Sample};

    fn seeded() -> Store {
        let store = Store::open_in_memory().unwrap();
        let t = Utc::now() - Duration::minutes(5);
        store
            .apply(
                "host",
                t,
                &Observation {
                    resources: vec![
                        Resource::new("host:h", ResourceKind::Host, "h").health(Health::Ok)
                    ],
                    samples: vec![Sample::new("host:h", "cpu.used_pct", 12.345)],
                },
            )
            .unwrap();
        store
            .apply(
                "systemd",
                t,
                &Observation {
                    resources: vec![
                        Resource::new("unit:h:ok.service", ResourceKind::Service, "ok.service")
                            .parent("host:h")
                            .health(Health::Ok),
                        Resource::new("unit:h:bad.service", ResourceKind::Service, "bad.service")
                            .parent("host:h")
                            .health(Health::Failed)
                            .attr("active", "failed"),
                    ],
                    samples: vec![],
                },
            )
            .unwrap();
        store
    }

    #[test]
    fn snapshot_puts_unhealthy_first() {
        let store = seeded();
        let s = snapshot(&store, SnapshotParams::default()).unwrap();
        assert_eq!(s.total, 3);
        assert_eq!(s.resources[0].id, "unit:h:bad.service");
        assert!(s.resources[0].attrs.is_some());
        assert_eq!(s.counts["service"].failed, 1);
        assert_eq!(s.as_of_seq, 3);
    }

    #[test]
    fn explain_includes_children_and_suggestions() {
        let store = seeded();
        let e = explain(
            &store,
            ExplainParams {
                id: "host:h".into(),
                changes_limit: None,
            },
        )
        .unwrap();
        assert_eq!(e.children.total, 2);
        assert_eq!(e.children.unhealthy.len(), 1);
        assert_eq!(e.metrics["cpu.used_pct"].value, 12.35);

        let err = explain(
            &store,
            ExplainParams {
                id: "unit:x:bad".into(),
                changes_limit: None,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("unit:h:bad.service"));
    }

    #[test]
    fn metrics_unknown_metric_lists_available() {
        let store = seeded();
        let err = metrics(
            &store,
            MetricsParams {
                id: "host:h".into(),
                metric: "nope".into(),
                range: None,
                step: None,
                agg: None,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("cpu.used_pct"));

        let s = metrics(
            &store,
            MetricsParams {
                id: "host:h".into(),
                metric: "cpu.used_pct".into(),
                range: Some("1h".into()),
                step: Some("1s".into()),
                agg: None,
            },
        )
        .unwrap();
        assert_eq!(s.step_secs, 12, "step widened to cap points at 300");
        assert_eq!(s.points.len(), 1);
    }

    #[test]
    fn changes_paginate_with_cursor() {
        let store = seeded();
        let page = changes(
            &store,
            ChangesParams {
                since: "seq:0".into(),
                kind: None,
                resource_prefix: None,
                limit: Some(2),
            },
        )
        .unwrap();
        assert!(page.truncated);
        assert_eq!(page.next_cursor, "seq:2");
        let rest = changes(
            &store,
            ChangesParams {
                since: page.next_cursor,
                kind: None,
                resource_prefix: None,
                limit: Some(2),
            },
        )
        .unwrap();
        assert!(!rest.truncated);
        assert_eq!(rest.changes.len(), 1);
        assert_eq!(rest.next_cursor, "seq:3");

        let err = changes(
            &store,
            ChangesParams {
                since: "nope".into(),
                kind: None,
                resource_prefix: None,
                limit: None,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("checkpoint_create"));
    }

    #[test]
    fn checkpoint_names_are_validated() {
        let store = seeded();
        assert!(checkpoint(&store, CheckpointParams { name: "15m".into() }).is_err());
        assert!(checkpoint(&store, CheckpointParams { name: "42".into() }).is_err());
        let cp = checkpoint(
            &store,
            CheckpointParams {
                name: "before".into(),
            },
        )
        .unwrap();
        assert_eq!(cp.seq, 3);
    }

    #[test]
    fn export_is_jsonl_with_meta() {
        let store = seeded();
        let out = export(
            &store,
            ExportParams {
                since: Some("seq:0".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let lines: Vec<Value> = out
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 3 + 3 + 1);
        assert_eq!(lines.last().unwrap()["type"], "meta");
    }
}
