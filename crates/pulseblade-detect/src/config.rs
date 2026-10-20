//! Detection settings. Missing keys keep the defaults so existing config files parse.

use pulseblade_core::Severity;
use serde::Deserialize;

fn warning() -> Severity {
    Severity::Warning
}

/// Comparison against a metric's latest value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum Compare {
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Lte,
}

impl Compare {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
        }
    }

    pub fn holds(self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => value > threshold,
            Self::Gte => value >= threshold,
            Self::Lt => value < threshold,
            Self::Lte => value <= threshold,
        }
    }
}

/// One threshold rule from config.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdRule {
    /// Optional stable name. Empty derives one from the metric and threshold.
    #[serde(default)]
    pub name: String,
    /// Resource id glob (`*` matches any run).
    #[serde(rename = "match")]
    pub pattern: String,
    pub metric: String,
    pub op: Compare,
    pub value: f64,
    #[serde(default = "warning")]
    pub severity: Severity,
}

/// Threshold rules, EWMA anomaly detection, and an optional findings webhook.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DetectConfig {
    /// When false, the collector skips detection and leaves existing findings alone.
    pub enabled: bool,
    /// Score configured metrics with an exponentially weighted z-score.
    pub ewma: bool,
    /// EWMA smoothing factor in `(0, 1]`. Higher reacts faster.
    pub ewma_alpha: f64,
    /// |z| above this, on the high side, opens an anomaly finding.
    pub ewma_z: f64,
    /// Samples required before an anomaly can fire.
    pub ewma_min_points: usize,
    /// Metrics watched for anomalies. Empty disables EWMA even when `ewma` is true.
    pub ewma_metrics: Vec<String>,
    /// POST a small JSON body here when new findings open. Empty disables it.
    pub webhook: String,
    pub rules: Vec<ThresholdRule>,
}

impl Default for DetectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ewma: true,
            ewma_alpha: 0.3,
            ewma_z: 5.0,
            ewma_min_points: 20,
            ewma_metrics: vec![
                "cpu.used_pct".into(),
                "mem.used_pct".into(),
                "disk.used_pct".into(),
                "journal.errors".into(),
            ],
            webhook: String::new(),
            rules: Vec::new(),
        }
    }
}

impl ThresholdRule {
    pub fn rule_name(&self) -> String {
        if !self.name.is_empty() {
            return self.name.clone();
        }
        format!(
            "threshold:{}{}{}",
            self.metric,
            self.op.as_str(),
            fmt_threshold(self.value)
        )
    }
}

fn fmt_threshold(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}
