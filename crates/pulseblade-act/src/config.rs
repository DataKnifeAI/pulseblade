//! Node-local action policy. Missing keys keep the built-in defaults.

use pulseblade_core::RiskTier;
use serde::{Deserialize, Serialize};

/// How long an approve-tier run waits for a human (`30s`, `15m`, `1h`, `1d`).
pub fn parse_ttl(raw: &str) -> Result<chrono::Duration, String> {
    let s = raw.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix('d') {
        (n, 86_400)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else {
        return Err(format!(
            "approval_ttl `{raw}` must look like 30s, 15m, 1h, or 1d"
        ));
    };
    let n: i64 = num
        .trim()
        .parse()
        .map_err(|_| format!("approval_ttl `{raw}` has no number"))?;
    if !(0..=30 * 86_400).contains(&(n.saturating_mul(mult))) {
        return Err("approval_ttl must be between 0s and 30d".into());
    }
    Ok(chrono::Duration::seconds(n.saturating_mul(mult)))
}

/// Tier and hourly cap shared by the built-in actions. `tier = null` keeps the default.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TierLimit {
    pub tier: Option<RiskTier>,
    pub rate_per_hour: u32,
}

impl Default for TierLimit {
    fn default() -> Self {
        Self {
            tier: None,
            rate_per_hour: 12,
        }
    }
}

/// An action whose targets must be listed explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PathAction {
    pub tier: Option<RiskTier>,
    pub rate_per_hour: u32,
    pub allow: Vec<String>,
}

impl Default for PathAction {
    fn default() -> Self {
        Self {
            tier: None,
            rate_per_hour: 4,
            allow: Vec::new(),
        }
    }
}

/// Policy for the built-in actions. The node enforces this; a hub cannot override it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ActionsConfig {
    pub approval_ttl: String,
    /// When set, `POST /api/v1/actions/{id}/approve` requires `Authorization: Bearer <token>`.
    pub approve_token: String,
    pub systemd_restart: TierLimit,
    pub docker_restart: TierLimit,
    pub disk_prune: PathAction,
    pub script_run: PathAction,
    pub proxmox_start: TierLimit,
    pub proxmox_stop: TierLimit,
    pub proxmox_migrate: TierLimit,
    pub k8s_rollout_restart: TierLimit,
    pub k8s_scale: TierLimit,
}

impl Default for ActionsConfig {
    fn default() -> Self {
        Self {
            approval_ttl: "1h".into(),
            approve_token: String::new(),
            systemd_restart: TierLimit::default(),
            docker_restart: TierLimit::default(),
            disk_prune: PathAction::default(),
            script_run: PathAction {
                tier: None,
                rate_per_hour: 4,
                allow: Vec::new(),
            },
            proxmox_start: TierLimit::default(),
            proxmox_stop: TierLimit::default(),
            proxmox_migrate: TierLimit::default(),
            k8s_rollout_restart: TierLimit::default(),
            k8s_scale: TierLimit::default(),
        }
    }
}
