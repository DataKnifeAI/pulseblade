//! Agent-shaped queries over the store, shared by the MCP tools, the HTTP API, and
//! `pulseblade ctl`.
//!
//! Responses are bounded, ordered by relevance, and always carry `as_of_seq` so
//! the caller can use it as its next `changes_since` cursor. Defaults favor small
//! responses: callers opt into detail rather than out of it.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, Utc};
use pulseblade_core::{
    parse_duration, ActionRun, Aggregation, Change, ChangeKind, Checkpoint, FindingStatus, Health,
    LogLine, Note, ResourceKind, Severity, Since, StoredResource,
};
use pulseblade_store::{ChangeFilter, FindingFilter, ResourceFilter, Store, StoreError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const DEFAULT_SNAPSHOT_LIMIT: usize = 50;
const MAX_SNAPSHOT_LIMIT: usize = 500;
const DEFAULT_CHANGES_LIMIT: usize = 100;
const MAX_CHANGES_LIMIT: usize = 1000;
const MAX_POINTS: i64 = 300;
/// Target points when the caller gives no `step`.
const DEFAULT_POINTS: i64 = 120;
const DEFAULT_EXPORT_LINES: usize = 2000;
const MAX_EXPORT_LINES: usize = 10_000;
/// Assumed collection interval when no collector run records one.
const DEFAULT_INTERVAL_SECS: u64 = 15;
/// A collector is stale after three intervals without a pass, but never sooner than this.
const MIN_STALE_SECS: i64 = 60;
/// Host metrics included in the summary snapshot.
const KEY_HOST_METRICS: &[&str] = &["cpu.used_pct", "mem.used_pct", "load.1m", "swap.used_pct"];

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Store(StoreError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    NotFound(String),
}

impl From<StoreError> for QueryError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::UnknownFinding(id) => Self::NotFound(format!("no finding `{id}`")),
            StoreError::Invalid(msg) => Self::Invalid(msg),
            other => Self::Store(other),
        }
    }
}

pub type Result<T> = std::result::Result<T, QueryError>;

fn is_false(b: &bool) -> bool {
    !*b
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

// ---------------------------------------------------------------------------
// collector health and status

/// Compact collector health, embedded in snapshots.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CollectorHealth {
    pub source: String,
    /// Last pass succeeded and is recent.
    pub ok: bool,
    /// Seconds since the last pass.
    pub age_secs: i64,
    /// No pass within three intervals: this collector's data is going stale.
    #[serde(default, skip_serializing_if = "is_false")]
    pub stale: bool,
    /// Error from the last pass, if it failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Full collector status for the HTTP API and `ctl status`.
#[derive(Debug, Clone, Serialize)]
pub struct CollectorStatus {
    pub source: String,
    /// Last pass succeeded and is recent.
    pub ok: bool,
    pub stale: bool,
    pub last_run: DateTime<Utc>,
    pub age_secs: i64,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success: Option<DateTime<Utc>>,
    pub consecutive_failures: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub resource_count: usize,
    pub sample_count: usize,
    pub change_count: usize,
}

impl CollectorStatus {
    pub fn health(&self) -> CollectorHealth {
        CollectorHealth {
            source: self.source.clone(),
            ok: self.ok,
            age_secs: self.age_secs,
            stale: self.stale,
            error: self.error.clone(),
        }
    }
}

/// Latest pass of every collector that has recorded one, by source.
pub fn collectors(store: &Store) -> Result<Vec<CollectorStatus>> {
    let now = Utc::now();
    Ok(store
        .collector_summaries()?
        .into_iter()
        .map(|s| {
            let age_secs = (now - s.last.ts).num_seconds().max(0);
            let interval = s.last.interval_secs.unwrap_or(DEFAULT_INTERVAL_SECS) as i64;
            let stale = age_secs > (3 * interval).max(MIN_STALE_SECS);
            CollectorStatus {
                ok: s.last.ok && !stale,
                stale,
                last_run: s.last.ts,
                age_secs,
                duration_ms: s.last.duration_ms,
                interval_secs: s.last.interval_secs,
                last_success: s.last_success,
                consecutive_failures: s.consecutive_failures,
                error: s.last.error,
                resource_count: s.last.resource_count,
                sample_count: s.last.sample_count,
                change_count: s.last.change_count,
                source: s.last.source,
            }
        })
        .collect())
}

#[derive(Debug, Serialize)]
pub struct DbStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ResourceTotals {
    pub total: usize,
    pub failed: usize,
    pub degraded: usize,
    pub by_kind: BTreeMap<String, KindCounts>,
}

/// Pulseblade's own health: what it is collecting, how fresh, and how big.
#[derive(Debug, Serialize)]
pub struct Status {
    pub version: &'static str,
    pub as_of_seq: i64,
    pub generated_at: DateTime<Utc>,
    /// Seconds since the serving process started, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_secs: Option<i64>,
    pub db: DbStatus,
    pub collectors: Vec<CollectorStatus>,
    pub resources: ResourceTotals,
    pub samples: u64,
    pub changes: u64,
    pub checkpoints: u64,
    /// Findings with status `open`.
    pub open_findings: u64,
    /// Action runs waiting for a human.
    pub awaiting_actions: u64,
}

/// Self-health. `process_started` is the serving process's start time, if any.
pub fn status(store: &Store, process_started: Option<DateTime<Utc>>) -> Result<Status> {
    let now = Utc::now();
    let as_of_seq = store.current_seq()?;
    let resources = store.resources(&ResourceFilter::default())?;
    let by_kind = count_by_kind(&resources);
    let stats = store.stats()?;
    Ok(Status {
        version: env!("CARGO_PKG_VERSION"),
        as_of_seq,
        generated_at: now,
        uptime_secs: process_started.map(|t| (now - t).num_seconds().max(0)),
        db: DbStatus {
            path: store.path().map(|p| p.display().to_string()),
            size_bytes: store.size_bytes(),
        },
        collectors: collectors(store)?,
        resources: ResourceTotals {
            total: resources.len(),
            failed: by_kind.values().map(|c| c.failed).sum(),
            degraded: by_kind.values().map(|c| c.degraded).sum(),
            by_kind,
        },
        samples: stats.samples,
        changes: stats.changes,
        checkpoints: stats.checkpoints,
        open_findings: store.open_finding_count()?,
        awaiting_actions: store.awaiting_action_count()?,
    })
}

// ---------------------------------------------------------------------------
// state_snapshot

/// How much a snapshot includes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Detail {
    /// Counts, collector health, host key metrics, and unhealthy resources only.
    Summary,
    /// Every matching resource with health and latest metrics; attributes for unhealthy ones.
    Brief,
    /// Every matching resource with all labels and attributes.
    Full,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct SnapshotParams {
    /// `summary` (default without filters), `brief` (default with filters), or `full`.
    #[serde(default)]
    pub detail: Option<Detail>,
    /// Return only `{as_of_seq, unchanged: true}` if the journal is still at this seq.
    #[serde(default)]
    pub if_changed_since: Option<i64>,
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
    /// Maximum resources returned (default 50, max 500). Unhealthy resources sort first.
    #[serde(default)]
    pub limit: Option<usize>,
}

impl SnapshotParams {
    fn filtered(&self) -> bool {
        self.kind.is_some()
            || self.query.is_some()
            || self.labels.as_ref().is_some_and(|l| !l.is_empty())
            || self.unhealthy_only == Some(true)
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ResourceBrief {
    pub id: String,
    pub kind: ResourceKind,
    /// Omitted in summary and brief detail when it equals the id's last segment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub health: Health,
    /// Semantic labels. Summary and brief detail omit `host`, which the id encodes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Latest metric values, rounded to two decimals.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metrics: BTreeMap<String, f64>,
    /// Attributes, present in full detail and for unhealthy resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attrs: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct KindCounts {
    pub total: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failed: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub degraded: usize,
}

#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct Snapshot {
    /// Journal position this snapshot reflects; pass to `changes_since` or `if_changed_since`.
    pub as_of_seq: i64,
    /// True when `if_changed_since` matched: nothing in the journal changed.
    #[serde(default, skip_serializing_if = "is_false")]
    pub unchanged: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Detail>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<DateTime<Utc>>,
    /// Collector health. When `unchanged`, only collectors that are not ok.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collectors: Vec<CollectorHealth>,
    /// Counts across all matching resources, keyed by kind.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub counts: BTreeMap<String, KindCounts>,
    /// Summary detail: hosts with key metrics.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<ResourceBrief>,
    /// Unhealthy first, then by kind and id. Summary detail lists unhealthy resources only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<ResourceBrief>,
    /// Resources selected before `limit` (summary: unhealthy ones).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
}

pub fn snapshot(store: &Store, p: SnapshotParams) -> Result<Snapshot> {
    let as_of_seq = store.current_seq()?;
    let collectors: Vec<CollectorHealth> = collectors(store)?.iter().map(|c| c.health()).collect();
    if p.if_changed_since == Some(as_of_seq) {
        return Ok(Snapshot {
            as_of_seq,
            unchanged: true,
            collectors: collectors.into_iter().filter(|c| !c.ok).collect(),
            ..Default::default()
        });
    }

    let detail = p.detail.unwrap_or(if p.filtered() {
        Detail::Brief
    } else {
        Detail::Summary
    });
    let filter = ResourceFilter {
        kind: p.kind,
        query: p.query,
        labels: p.labels.unwrap_or_default(),
        unhealthy_only: p.unhealthy_only.unwrap_or(false),
        include_absent: false,
    };
    let mut all = store.resources(&filter)?;
    let latest = store.latest_all()?;
    let limit = p
        .limit
        .unwrap_or(DEFAULT_SNAPSHOT_LIMIT)
        .clamp(1, MAX_SNAPSHOT_LIMIT);
    let counts = count_by_kind(&all);
    all.sort_by(|a, b| {
        (a.resource.health, a.resource.kind, &a.resource.id).cmp(&(
            b.resource.health,
            b.resource.kind,
            &b.resource.id,
        ))
    });

    let mut hosts = Vec::new();
    let selected: Vec<StoredResource> = match detail {
        Detail::Summary => {
            hosts = all
                .iter()
                .filter(|r| r.resource.kind == ResourceKind::Host)
                .map(|r| brief(r.clone(), &latest, BriefOpts::HOST_KEY))
                .collect();
            all.into_iter()
                .filter(|r| r.resource.health.is_unhealthy())
                .collect()
        }
        Detail::Brief | Detail::Full => all,
    };
    let opts = match detail {
        Detail::Summary => BriefOpts::UNHEALTHY,
        Detail::Brief => BriefOpts::BRIEF,
        Detail::Full => BriefOpts::FULL,
    };
    let total = selected.len();
    let resources = selected
        .into_iter()
        .take(limit)
        .map(|r| brief(r, &latest, opts))
        .collect();

    Ok(Snapshot {
        as_of_seq,
        unchanged: false,
        detail: Some(detail),
        generated_at: (detail != Detail::Summary).then(Utc::now),
        collectors,
        counts,
        hosts,
        resources,
        total: Some(total),
        truncated: total > limit,
    })
}

fn count_by_kind(resources: &[StoredResource]) -> BTreeMap<String, KindCounts> {
    let mut counts: BTreeMap<String, KindCounts> = BTreeMap::new();
    for r in resources {
        let c = counts.entry(r.resource.kind.to_string()).or_default();
        c.total += 1;
        match r.resource.health {
            Health::Failed => c.failed += 1,
            Health::Degraded => c.degraded += 1,
            _ => {}
        }
    }
    counts
}

#[derive(Debug, Clone, Copy)]
struct BriefOpts {
    /// Attributes for every resource, not only unhealthy ones.
    all_attrs: bool,
    /// Drop the `host` label and a name that repeats the id.
    compact: bool,
    /// Only these metrics, when set.
    metrics: Option<&'static [&'static str]>,
}

impl BriefOpts {
    const HOST_KEY: Self = Self {
        all_attrs: false,
        compact: true,
        metrics: Some(KEY_HOST_METRICS),
    };
    const UNHEALTHY: Self = Self {
        all_attrs: true,
        compact: true,
        metrics: None,
    };
    const BRIEF: Self = Self {
        all_attrs: false,
        compact: true,
        metrics: None,
    };
    const FULL: Self = Self {
        all_attrs: true,
        compact: false,
        metrics: None,
    };
}

fn brief(
    r: StoredResource,
    latest: &HashMap<String, BTreeMap<String, f64>>,
    opts: BriefOpts,
) -> ResourceBrief {
    let mut res = r.resource;
    let metrics = latest
        .get(&res.id)
        .map(|m| {
            m.iter()
                .filter(|(k, _)| opts.metrics.is_none_or(|keep| keep.contains(&k.as_str())))
                .map(|(k, v)| (k.clone(), round2(*v)))
                .collect()
        })
        .unwrap_or_default();
    let attrs = (opts.all_attrs || res.health.is_unhealthy()).then_some(res.attrs);
    let name = if opts.compact && name_is_redundant(&res.id, &res.name) {
        None
    } else {
        Some(res.name)
    };
    if opts.compact {
        res.labels.remove("host");
    }
    ResourceBrief {
        id: res.id,
        kind: res.kind,
        name,
        health: res.health,
        labels: res.labels,
        metrics,
        attrs,
    }
}

/// `unit:web1:sshd.service` already says `sshd.service`.
fn name_is_redundant(id: &str, name: &str) -> bool {
    id.strip_suffix(name).is_some_and(|p| p.ends_with(':'))
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
    /// Notes attached to this resource, newest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<Note>,
    /// Open and acked findings on this resource. Context and logs are included.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub findings: Vec<FindingView>,
    /// Recent action runs filed on this resource, newest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ActionRun>,
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

    let parent_opts = BriefOpts {
        all_attrs: false,
        ..BriefOpts::FULL
    };
    let parent = match &resource.resource.parent {
        Some(pid) => store
            .resource(pid)?
            .map(|r| brief(r, &latest_all, parent_opts)),
        None => None,
    };

    let children = store.children(&p.id)?;
    let by_kind = count_by_kind(&children);
    let total = children.len();
    let unhealthy = children
        .into_iter()
        .filter(|c| c.resource.health.is_unhealthy())
        .map(|c| brief(c, &latest_all, BriefOpts::FULL))
        .collect();

    let mut ids = vec![p.id.clone()];
    ids.extend(resource.resource.parent.clone());
    let recent_changes = store.recent_changes(&ids, p.changes_limit.unwrap_or(20).min(200))?;
    let notes = store.notes_for(&p.id, 10)?;
    let (matched, _) = store.findings(
        &FindingFilter {
            resource_id: Some(p.id.clone()),
            ..Default::default()
        },
        20,
    )?;
    let findings = matched
        .into_iter()
        .filter(|f| matches!(f.status, FindingStatus::Open | FindingStatus::Acked))
        .take(5)
        .map(|f| finding_view(f, true))
        .collect();
    let actions = store.recent_actions(Some(&p.id), 8)?;

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
        notes,
        findings,
        actions,
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
    /// Bucket width such as `30s` or `5m`. Default targets ~120 points; widened to keep at most 300.
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

/// A regular series: `values[i]` covers the bucket starting at `start + i * step_secs`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct Series {
    pub id: String,
    pub metric: String,
    pub agg: Aggregation,
    /// Start of the first bucket with data; absent when the range has no samples.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<DateTime<Utc>>,
    pub step_secs: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<SeriesStats>,
    /// One value per bucket, from the first to the last bucket with data; null where empty.
    pub values: Vec<Option<f64>>,
}

pub fn metrics(store: &Store, p: MetricsParams) -> Result<Series> {
    let range = parse_duration(p.range.as_deref().unwrap_or("1h"))
        .map_err(|e| QueryError::Invalid(e.to_string()))?;
    if range <= Duration::zero() {
        return Err(QueryError::Invalid("range must be positive".into()));
    }
    let range_ms = range.num_milliseconds();
    let min_step_ms = (range_ms / MAX_POINTS).max(1000);
    let step_ms = match p.step.as_deref() {
        Some(s) => parse_duration(s)
            .map_err(|e| QueryError::Invalid(e.to_string()))?
            .num_milliseconds(),
        None => (range_ms / DEFAULT_POINTS).max(sample_interval_ms(store)?),
    }
    .max(min_step_ms);
    // Whole seconds so `step_secs` is exact.
    let step_ms = (step_ms + 999) / 1000 * 1000;

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
    let buckets = store.series(&p.id, &p.metric, from, to, step_ms, agg)?;
    let (start, values) = match (buckets.first(), buckets.last()) {
        (Some(first), Some(last)) => {
            let first_ms = first.ts.timestamp_millis();
            let n = (last.ts.timestamp_millis() - first_ms) / step_ms + 1;
            let mut values = vec![None; n as usize];
            for b in &buckets {
                values[((b.ts.timestamp_millis() - first_ms) / step_ms) as usize] =
                    Some(round2(b.value));
            }
            (Some(first.ts), values)
        }
        _ => (None, Vec::new()),
    };
    let present: Vec<f64> = values.iter().flatten().copied().collect();
    let stats = (!present.is_empty()).then(|| SeriesStats {
        min: present.iter().copied().fold(f64::INFINITY, f64::min),
        max: present.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        avg: round2(present.iter().sum::<f64>() / present.len() as f64),
        last: present[present.len() - 1],
    });

    Ok(Series {
        id: p.id,
        metric: p.metric,
        agg,
        start,
        step_secs: step_ms / 1000,
        stats,
        values,
    })
}

/// Widest collection interval among collectors, so default buckets are rarely empty.
fn sample_interval_ms(store: &Store) -> Result<i64> {
    let secs = store
        .collector_summaries()?
        .iter()
        .filter_map(|s| s.last.interval_secs)
        .max()
        .unwrap_or(DEFAULT_INTERVAL_SECS);
    Ok(secs as i64 * 1000)
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

pub fn checkpoints(store: &Store) -> Result<Vec<Checkpoint>> {
    Ok(store.checkpoints()?)
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
    /// Omit per-change `ts` (default true); `first_ts`/`last_ts` bound the set. False adds `ts` to each change.
    #[serde(default)]
    pub compact: Option<bool>,
}

#[derive(Debug, Default, Serialize, JsonSchema)]
pub struct ChangeCounts {
    pub appeared: usize,
    pub changed: usize,
    pub disappeared: usize,
}

/// A journal entry; `ts` is present only when `compact` is false.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ChangeEntry {
    pub seq: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts: Option<DateTime<Utc>>,
    pub resource_id: String,
    pub kind: ChangeKind,
    /// `health`, `name`, `parent`, `attrs.<key>`, or `labels.<key>` for `changed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Value>,
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
    /// Time of the oldest returned change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_ts: Option<DateTime<Utc>>,
    /// Time of the newest returned change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ts: Option<DateTime<Utc>>,
    pub changes: Vec<ChangeEntry>,
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
    let compact = p.compact.unwrap_or(true);
    Ok(ChangeSet {
        from_seq,
        as_of_seq,
        next_cursor: format!("seq:{next}"),
        total,
        truncated,
        counts,
        first_ts: changes.first().map(|c| c.ts),
        last_ts: changes.last().map(|c| c.ts),
        changes: changes
            .into_iter()
            .map(|c| ChangeEntry {
                seq: c.seq,
                ts: (!compact).then_some(c.ts),
                resource_id: c.resource_id,
                kind: c.kind,
                field: c.field,
                before: c.before,
                after: c.after,
            })
            .collect(),
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

// ---------------------------------------------------------------------------
// findings, notes, logs

const DEFAULT_FINDINGS: usize = 20;
const MAX_FINDINGS: usize = 100;
const DEFAULT_LOGS: usize = 20;
const MAX_LOGS: usize = 100;
const DEFAULT_NOTES: usize = 10;
const MAX_NOTES: usize = 50;
const MAX_NOTE_CHARS: usize = 2000;

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct FindingsParams {
    /// `open` (default), `acked`, or `resolved`.
    #[serde(default)]
    pub status: Option<FindingStatus>,
    #[serde(default)]
    pub severity: Option<Severity>,
    /// Maximum findings (default 20, max 100). Critical first.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Include causal context and recent error lines. Default false.
    #[serde(default)]
    pub detail: Option<bool>,
}

/// A finding as returned to agents. Context and logs are omitted unless requested.
#[derive(Debug, Serialize, JsonSchema)]
pub struct FindingView {
    pub id: String,
    pub resource_id: String,
    pub rule: String,
    pub severity: Severity,
    pub status: FindingStatus,
    pub summary: String,
    pub opened_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<Change>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<LogLine>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FindingPage {
    pub as_of_seq: i64,
    pub total: usize,
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
    pub findings: Vec<FindingView>,
}

pub fn findings(store: &Store, p: FindingsParams) -> Result<FindingPage> {
    let limit = p.limit.unwrap_or(DEFAULT_FINDINGS).clamp(1, MAX_FINDINGS);
    let detail = p.detail.unwrap_or(false);
    let (rows, total) = store.findings(
        &FindingFilter {
            status: Some(p.status.unwrap_or(FindingStatus::Open)),
            severity: p.severity,
            resource_id: None,
        },
        limit,
    )?;
    Ok(FindingPage {
        as_of_seq: store.current_seq()?,
        total,
        truncated: total > rows.len(),
        findings: rows.into_iter().map(|f| finding_view(f, detail)).collect(),
    })
}

fn finding_view(f: pulseblade_core::Finding, detail: bool) -> FindingView {
    FindingView {
        id: f.id,
        resource_id: f.resource_id,
        rule: f.rule,
        severity: f.severity,
        status: f.status,
        summary: f.summary,
        opened_at: f.opened_at,
        context: if detail { f.context } else { Vec::new() },
        logs: if detail { f.logs } else { Vec::new() },
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AckParams {
    pub id: String,
    /// Optional note recorded on the finding's resource.
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct AckResult {
    pub as_of_seq: i64,
    pub finding: FindingView,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<Note>,
}

pub fn ack_finding(store: &Store, p: AckParams) -> Result<AckResult> {
    let finding = store.ack_finding(&p.id, Utc::now())?;
    let note = match p.note.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        Some(text) => Some(write_note(store, &finding.resource_id, None, text)?),
        None => None,
    };
    Ok(AckResult {
        as_of_seq: store.current_seq()?,
        finding: finding_view(finding, true),
        note,
    })
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct NoteParams {
    /// Resource id the note is about.
    pub resource: String,
    pub text: String,
    /// Who wrote it (default `agent`).
    #[serde(default)]
    pub author: Option<String>,
}

pub fn memory_note(store: &Store, p: NoteParams) -> Result<Note> {
    write_note(store, &p.resource, p.author.as_deref(), &p.text)
}

fn write_note(store: &Store, resource: &str, author: Option<&str>, text: &str) -> Result<Note> {
    let text = text.trim();
    if text.is_empty() {
        return Err(QueryError::Invalid("note text is empty".into()));
    }
    if text.chars().count() > MAX_NOTE_CHARS {
        return Err(QueryError::Invalid(format!(
            "note text is longer than {MAX_NOTE_CHARS} characters"
        )));
    }
    if store.resource(resource)?.is_none() {
        return Err(not_found_with_suggestions(store, resource)?);
    }
    let author = author.unwrap_or("agent").trim();
    let author = if author.is_empty() { "agent" } else { author };
    let author = author.chars().take(64).collect::<String>();
    Ok(store.insert_note(resource, &author, text, Utc::now())?)
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchParams {
    /// Case-insensitive substring of note text or resource id.
    pub query: String,
    /// Maximum notes (default 10, max 50).
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct NotePage {
    pub as_of_seq: i64,
    pub total: usize,
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
    pub notes: Vec<Note>,
}

pub fn memory_search(store: &Store, p: SearchParams) -> Result<NotePage> {
    let query = p.query.trim();
    if query.is_empty() {
        return Err(QueryError::Invalid("query is empty".into()));
    }
    let limit = p.limit.unwrap_or(DEFAULT_NOTES).clamp(1, MAX_NOTES);
    let (notes, total) = store.search_notes(query, limit)?;
    Ok(NotePage {
        as_of_seq: store.current_seq()?,
        total,
        truncated: total > notes.len(),
        notes,
    })
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LogsParams {
    /// Resource id, e.g. `unit:web1:sshd.service`.
    pub resource: String,
    /// Look-back window such as `15m` or an RFC 3339 timestamp (default `1h`).
    #[serde(default)]
    pub since: Option<String>,
    /// Maximum lines, newest first (default 20, max 100).
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct LogPage {
    pub as_of_seq: i64,
    pub resource: String,
    pub total: usize,
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
    /// Recent error lines. Empty when the journal has been quiet.
    pub lines: Vec<LogLine>,
}

pub fn logs(store: &Store, p: LogsParams) -> Result<LogPage> {
    let since = match Since::parse(p.since.as_deref().unwrap_or("1h"), Utc::now()) {
        Since::Time(t) => t,
        Since::Seq(_) | Since::Checkpoint(_) => {
            return Err(QueryError::Invalid(
                "since must be a duration like 15m or an RFC 3339 timestamp".into(),
            ));
        }
    };
    let limit = p.limit.unwrap_or(DEFAULT_LOGS).clamp(1, MAX_LOGS);
    let (lines, total) = store.logs_since(&p.resource, since, limit)?;
    if lines.is_empty() && store.resource(&p.resource)?.is_none() {
        return Err(not_found_with_suggestions(store, &p.resource)?);
    }
    Ok(LogPage {
        as_of_seq: store.current_seq()?,
        resource: p.resource,
        total,
        truncated: total > lines.len(),
        lines,
    })
}

// ---------------------------------------------------------------------------
// fleet_overview

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct FleetParams {
    /// Limit the overview to one host or node name.
    #[serde(default)]
    pub host: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FleetGroup {
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub criticality: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub ok: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub degraded: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failed: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub unknown: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct FleetOverview {
    pub as_of_seq: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub hosts: usize,
    /// Failed and degraded resources, with attributes. Healthy resources are counts only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unhealthy: Vec<ResourceBrief>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<FleetGroup>,
}

pub fn fleet_overview(store: &Store, p: FleetParams) -> Result<FleetOverview> {
    let all = store.resources(&ResourceFilter::default())?;
    let latest = store.latest_all()?;
    let mut unhealthy = Vec::new();
    let mut groups: BTreeMap<(String, String, String, String), [usize; 4]> = BTreeMap::new();
    let mut hosts = BTreeMap::<String, ()>::new();
    for stored in all {
        let res = &stored.resource;
        let host = res
            .labels
            .get("host")
            .map(String::as_str)
            .or_else(|| pulseblade_core::host_of_id(&res.id))
            .unwrap_or("unknown")
            .to_string();
        if p.host.as_ref().is_some_and(|want| want != &host) {
            continue;
        }
        hosts.insert(host.clone(), ());
        let role = res.labels.get("role").cloned().unwrap_or_default();
        let cluster = res.labels.get("cluster").cloned().unwrap_or_default();
        let criticality = res.labels.get("criticality").cloned().unwrap_or_default();
        let slot = match res.health {
            Health::Ok => 0,
            Health::Degraded => 1,
            Health::Failed => 2,
            Health::Unknown => 3,
        };
        *groups
            .entry((host, role, cluster, criticality))
            .or_default()
            .get_mut(slot)
            .expect("slot") += 1;
        if res.health.is_unhealthy() {
            unhealthy.push(brief(stored, &latest, BriefOpts::FULL));
        }
    }
    unhealthy.sort_by(|a, b| a.id.cmp(&b.id));
    let groups = groups
        .into_iter()
        .map(|((host, role, cluster, criticality), counts)| FleetGroup {
            host,
            role: nonempty(role),
            cluster: nonempty(cluster),
            criticality: nonempty(criticality),
            ok: counts[0],
            degraded: counts[1],
            failed: counts[2],
            unknown: counts[3],
        })
        .collect();
    Ok(FleetOverview {
        as_of_seq: store.current_seq()?,
        hosts: hosts.len(),
        unhealthy,
        groups,
    })
}

fn nonempty(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// JSON document for the `pulseblade://state` MCP resource.
pub fn state_document(store: &Store) -> Result<String> {
    let snap = snapshot(
        store,
        SnapshotParams {
            detail: Some(Detail::Summary),
            ..Default::default()
        },
    )?;
    serde_json::to_string(&snap).map_err(|e| QueryError::Invalid(e.to_string()))
}

/// JSON document for the `pulseblade://findings` MCP resource: open findings, no logs.
pub fn findings_document(store: &Store) -> Result<String> {
    let page = findings(
        store,
        FindingsParams {
            limit: Some(DEFAULT_FINDINGS),
            ..Default::default()
        },
    )?;
    serde_json::to_string(&page).map_err(|e| QueryError::Invalid(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulseblade_core::{CollectorRun, Observation, Resource, Sample};

    fn seeded() -> Store {
        let store = Store::open_in_memory().unwrap();
        let t = Utc::now() - Duration::minutes(5);
        store
            .apply(
                "host",
                t,
                &Observation {
                    resources: vec![Resource::new("host:h", ResourceKind::Host, "h")
                        .health(Health::Ok)
                        .label("host", "h")],
                    samples: vec![
                        Sample::new("host:h", "cpu.used_pct", 12.345),
                        Sample::new("host:h", "load.5m", 1.0),
                    ],
                    logs: vec![],
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
                            .health(Health::Ok)
                            .label("host", "h")
                            .attr("active", "active"),
                        Resource::new("unit:h:bad.service", ResourceKind::Service, "bad.service")
                            .parent("host:h")
                            .health(Health::Failed)
                            .label("host", "h")
                            .attr("active", "failed"),
                    ],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        store
    }

    fn record(store: &Store, source: &str, age: Duration, ok: bool) {
        store
            .record_run(&CollectorRun {
                source: source.into(),
                ts: Utc::now() - age,
                duration_ms: 3,
                ok,
                error: (!ok).then(|| "systemctl: boom".to_string()),
                resource_count: 1,
                sample_count: 0,
                change_count: 0,
                interval_secs: Some(15),
            })
            .unwrap();
    }

    #[test]
    fn snapshot_brief_puts_unhealthy_first() {
        let store = seeded();
        let s = snapshot(
            &store,
            SnapshotParams {
                detail: Some(Detail::Brief),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(s.total, Some(3));
        assert_eq!(s.resources[0].id, "unit:h:bad.service");
        assert!(s.resources[0].attrs.is_some());
        assert!(s.resources[1].attrs.is_none());
        assert!(s.resources.iter().all(|r| r.name.is_none()));
        assert!(s.resources.iter().all(|r| !r.labels.contains_key("host")));
        assert_eq!(s.counts["service"].failed, 1);
        assert_eq!(s.as_of_seq, 3);
    }

    #[test]
    fn snapshot_detail_defaults_and_modes() {
        let store = seeded();
        let summary = snapshot(&store, SnapshotParams::default()).unwrap();
        assert_eq!(summary.detail, Some(Detail::Summary));
        assert_eq!(summary.total, Some(1));
        assert_eq!(summary.resources.len(), 1);
        assert_eq!(summary.resources[0].id, "unit:h:bad.service");
        assert_eq!(summary.hosts.len(), 1);
        assert!(summary.hosts[0].metrics.contains_key("cpu.used_pct"));
        assert!(!summary.hosts[0].metrics.contains_key("load.5m"));
        assert!(summary.generated_at.is_none());

        let filtered = snapshot(
            &store,
            SnapshotParams {
                kind: Some(ResourceKind::Service),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(filtered.detail, Some(Detail::Brief));
        assert_eq!(filtered.resources.len(), 2);

        let full = snapshot(
            &store,
            SnapshotParams {
                detail: Some(Detail::Full),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(full.resources.iter().all(|r| r.attrs.is_some()));
        assert!(full.resources.iter().all(|r| r.name.is_some()));
        assert!(full.resources.iter().all(|r| r.labels["host"] == "h"));
    }

    #[test]
    fn snapshot_if_changed_since() {
        let store = seeded();
        record(&store, "host", Duration::seconds(5), true);
        let s = snapshot(
            &store,
            SnapshotParams {
                if_changed_since: Some(3),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(s.unchanged);
        assert!(s.resources.is_empty() && s.counts.is_empty() && s.collectors.is_empty());
        assert_eq!(
            serde_json::to_value(&s).unwrap(),
            json!({"as_of_seq": 3, "unchanged": true})
        );

        record(&store, "systemd", Duration::seconds(5), false);
        let s = snapshot(
            &store,
            SnapshotParams {
                if_changed_since: Some(3),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(s.collectors.len(), 1, "failing collectors still surface");

        let s = snapshot(
            &store,
            SnapshotParams {
                if_changed_since: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!s.unchanged);
        assert_eq!(s.collectors.len(), 2);
    }

    #[test]
    fn collectors_report_stale_and_failing() {
        let store = seeded();
        record(&store, "host", Duration::minutes(10), true);
        record(&store, "systemd", Duration::seconds(2), false);
        let c = collectors(&store).unwrap();
        assert!(c[0].stale && !c[0].ok);
        assert!(!c[1].stale && !c[1].ok);
        assert_eq!(c[1].consecutive_failures, 1);
        let st = status(&store, Some(Utc::now() - Duration::seconds(30))).unwrap();
        assert_eq!(st.resources.total, 3);
        assert_eq!(st.resources.failed, 1);
        assert!(st.uptime_secs.unwrap() >= 30);
        assert_eq!(st.collectors.len(), 2);
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
        assert_eq!(e.children.unhealthy[0].labels["host"], "h");
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
        assert_eq!(s.values, vec![Some(12.35)]);
    }

    #[test]
    fn metrics_compact_series_marks_gaps() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        // Samples 3 and 4 minutes ago and now, with an empty minute between.
        for (ago, v) in [(4, 1.0), (3, 2.0), (1, 4.0)] {
            store
                .apply(
                    "host",
                    now - Duration::minutes(ago),
                    &Observation {
                        resources: vec![Resource::new("host:h", ResourceKind::Host, "h")],
                        samples: vec![Sample::new("host:h", "cpu.used_pct", v)],
                        logs: vec![],
                    },
                )
                .unwrap();
        }
        let s = metrics(
            &store,
            MetricsParams {
                id: "host:h".into(),
                metric: "cpu.used_pct".into(),
                range: Some("10m".into()),
                step: Some("1m".into()),
                agg: None,
            },
        )
        .unwrap();
        assert_eq!(s.step_secs, 60);
        assert_eq!(s.values, vec![Some(1.0), Some(2.0), None, Some(4.0)]);
        let start = s.start.unwrap().timestamp_millis();
        assert_eq!(start % 60_000, 0, "buckets are aligned");
        let stats = s.stats.unwrap();
        assert_eq!((stats.min, stats.max, stats.last), (1.0, 4.0, 4.0));
        assert_eq!(stats.avg, 2.33);
    }

    #[test]
    fn metrics_default_step_respects_collection_interval() {
        let store = seeded();
        record(&store, "host", Duration::seconds(1), true);
        let s = metrics(
            &store,
            MetricsParams {
                id: "host:h".into(),
                metric: "cpu.used_pct".into(),
                range: Some("15m".into()),
                step: None,
                agg: None,
            },
        )
        .unwrap();
        assert_eq!(s.step_secs, 15);
        let s = metrics(
            &store,
            MetricsParams {
                id: "host:h".into(),
                metric: "cpu.used_pct".into(),
                range: Some("1h".into()),
                step: None,
                agg: None,
            },
        )
        .unwrap();
        assert_eq!(s.step_secs, 30);
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
                compact: None,
            },
        )
        .unwrap();
        assert!(page.truncated);
        assert_eq!(page.next_cursor, "seq:2");
        assert!(page.changes.iter().all(|c| c.ts.is_none()));
        assert!(page.first_ts.is_some() && page.last_ts.is_some());
        let rest = changes(
            &store,
            ChangesParams {
                since: page.next_cursor,
                kind: None,
                resource_prefix: None,
                limit: Some(2),
                compact: Some(false),
            },
        )
        .unwrap();
        assert!(!rest.truncated);
        assert_eq!(rest.changes.len(), 1);
        assert!(rest.changes[0].ts.is_some());
        assert_eq!(rest.next_cursor, "seq:3");

        let err = changes(
            &store,
            ChangesParams {
                since: "nope".into(),
                kind: None,
                resource_prefix: None,
                limit: None,
                compact: None,
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
        assert_eq!(checkpoints(&store).unwrap().len(), 1);
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

    #[test]
    fn fleet_overview_groups_labels_and_lists_unhealthy() {
        let store = Store::open_in_memory().unwrap();
        store
            .apply(
                "host",
                Utc::now(),
                &Observation {
                    resources: vec![
                        Resource::new("host:web1", ResourceKind::Host, "web1")
                            .health(Health::Ok)
                            .label("role", "edge")
                            .label("cluster", "prod")
                            .label("criticality", "high"),
                        Resource::new(
                            "unit:web1:sshd.service",
                            ResourceKind::Service,
                            "sshd.service",
                        )
                        .parent("host:web1")
                        .health(Health::Failed)
                        .label("role", "edge")
                        .label("criticality", "high"),
                        Resource::new("host:db1", ResourceKind::Host, "db1")
                            .health(Health::Ok)
                            .label("role", "data")
                            .label("cluster", "prod"),
                    ],
                    samples: vec![],
                    logs: vec![],
                },
            )
            .unwrap();
        let overview = fleet_overview(&store, FleetParams::default()).unwrap();
        assert_eq!(overview.hosts, 2);
        assert_eq!(overview.unhealthy.len(), 1);
        assert_eq!(overview.unhealthy[0].id, "unit:web1:sshd.service");
        assert!(overview.unhealthy[0].attrs.is_some());
        let edge = overview
            .groups
            .iter()
            .find(|g| g.host == "web1" && g.role.as_deref() == Some("edge") && g.failed == 1)
            .unwrap();
        assert_eq!(edge.criticality.as_deref(), Some("high"));
        assert!(overview.groups.iter().any(|g| g.host == "db1" && g.ok == 1));
        let one = fleet_overview(
            &store,
            FleetParams {
                host: Some("db1".into()),
            },
        )
        .unwrap();
        assert_eq!(one.hosts, 1);
        assert!(one.unhealthy.is_empty());
    }
}
