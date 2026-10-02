use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What sort of thing a resource is.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Host,
    Disk,
    NetIface,
    Service,
    Container,
    Vm,
    Node,
    Pod,
}

impl ResourceKind {
    pub const ALL: [ResourceKind; 8] = [
        Self::Host,
        Self::Disk,
        Self::NetIface,
        Self::Service,
        Self::Container,
        Self::Vm,
        Self::Node,
        Self::Pod,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Disk => "disk",
            Self::NetIface => "net_iface",
            Self::Service => "service",
            Self::Container => "container",
            Self::Vm => "vm",
            Self::Node => "node",
            Self::Pod => "pod",
        }
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ResourceKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| format!("unknown resource kind `{s}`"))
    }
}

/// Coarse health as judged by the collector that owns the resource.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    Failed,
    Degraded,
    #[default]
    Unknown,
    Ok,
}

impl Health {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Degraded => "degraded",
            Self::Unknown => "unknown",
            Self::Ok => "ok",
        }
    }

    pub fn is_unhealthy(self) -> bool {
        matches!(self, Self::Failed | Self::Degraded)
    }
}

impl FromStr for Health {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "failed" => Ok(Self::Failed),
            "degraded" => Ok(Self::Degraded),
            "unknown" => Ok(Self::Unknown),
            "ok" => Ok(Self::Ok),
            other => Err(format!("unknown health `{other}`")),
        }
    }
}

/// Anything observable: a host, a disk, a service, a container.
///
/// `attrs` holds slow-moving state facts and is change-tracked. Fast-moving
/// numbers belong in [`Sample`]s so they never produce change noise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Resource {
    /// Stable id, e.g. `host:web1`, `disk:web1:/`, `unit:web1:sshd.service`.
    pub id: String,
    pub kind: ResourceKind,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub health: Health,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attrs: BTreeMap<String, Value>,
}

impl Resource {
    pub fn new(id: impl Into<String>, kind: ResourceKind, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind,
            name: name.into(),
            parent: None,
            health: Health::Unknown,
            labels: BTreeMap::new(),
            attrs: BTreeMap::new(),
        }
    }

    pub fn parent(mut self, parent: impl Into<String>) -> Self {
        self.parent = Some(parent.into());
        self
    }

    pub fn health(mut self, health: Health) -> Self {
        self.health = health;
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    pub fn attr(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.attrs.insert(key.into(), value.into());
        self
    }
}

/// A resource as persisted, with bookkeeping added by the store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StoredResource {
    #[serde(flatten)]
    pub resource: Resource,
    /// Collector that owns this resource.
    pub source: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    /// False once the owning collector stops reporting it.
    pub present: bool,
}

/// One numeric observation. The timestamp comes from the enclosing [`Observation`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Sample {
    pub resource_id: String,
    pub metric: String,
    pub value: f64,
}

impl Sample {
    pub fn new(resource_id: impl Into<String>, metric: impl Into<String>, value: f64) -> Self {
        Self {
            resource_id: resource_id.into(),
            metric: metric.into(),
            value,
        }
    }
}

/// Everything one collector saw in one pass. `resources` is the complete set the
/// collector owns, so anything missing is treated as disappeared.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Observation {
    pub resources: Vec<Resource>,
    pub samples: Vec<Sample>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Appeared,
    Disappeared,
    Changed,
}

impl ChangeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Appeared => "appeared",
            Self::Disappeared => "disappeared",
            Self::Changed => "changed",
        }
    }
}

impl FromStr for ChangeKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "appeared" => Ok(Self::Appeared),
            "disappeared" => Ok(Self::Disappeared),
            "changed" => Ok(Self::Changed),
            other => Err(format!("unknown change kind `{other}`")),
        }
    }
}

/// Append-only change journal entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Change {
    /// Monotonic position in the journal; usable as a `changes_since` cursor.
    pub seq: i64,
    pub ts: DateTime<Utc>,
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

/// A named position in the change journal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Checkpoint {
    pub name: String,
    pub seq: i64,
    pub ts: DateTime<Utc>,
}

/// A point in a (possibly downsampled) time series.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Point {
    pub ts: DateTime<Utc>,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    #[default]
    Avg,
    Min,
    Max,
    Last,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FindingStatus {
    Open,
    Acked,
    Resolved,
}

/// A detected problem with the context an agent needs to reason about cause (M2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Finding {
    pub id: String,
    pub resource_id: String,
    pub rule: String,
    pub severity: Severity,
    pub status: FindingStatus,
    pub summary: String,
    pub opened_at: DateTime<Utc>,
    /// Changes on the resource and its relatives shortly before the finding opened.
    pub context: Vec<Change>,
}

/// How much human oversight an action requires (M3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RiskTier {
    /// Runs immediately when executed.
    Auto,
    /// Waits for a human approval before running.
    Approve,
    /// Never runs; visible so agents can explain why.
    Deny,
}

/// A remediation primitive an agent can plan and execute (M3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActionSpec {
    pub name: String,
    pub description: String,
    pub tier: RiskTier,
    /// JSON Schema for the action's parameters.
    pub params_schema: Value,
    /// Resource kinds the action applies to.
    pub applies_to: Vec<ResourceKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionRunStatus {
    Planned,
    AwaitingApproval,
    Running,
    Succeeded,
    Failed,
    Rejected,
}

/// One planned or executed action, recorded in the audit chain (M3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActionRun {
    pub id: String,
    pub action: String,
    pub resource_id: String,
    pub params: Value,
    pub idempotency_key: String,
    pub status: ActionRunStatus,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

/// Free-form memory attached to a resource by an agent or human (M2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Note {
    pub id: String,
    pub resource_id: String,
    pub author: String,
    pub text: String,
    pub created_at: DateTime<Utc>,
}
