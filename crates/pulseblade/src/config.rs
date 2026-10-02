use std::path::{Path, PathBuf};

use anyhow::Context;
use pulseblade_core::LabelRule;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Seconds between collector passes.
    pub interval_secs: u64,
    /// How long raw samples are kept.
    pub retention_hours: u64,
    /// How long change journal entries are kept.
    pub change_retention_days: u64,
    /// Address for the agent's HTTP MCP endpoint.
    pub listen: String,
    /// Extra `Host` header values accepted by the HTTP endpoint (loopback is always allowed).
    pub allowed_hosts: Vec<String>,
    /// Semantic labels applied to matching resource ids.
    pub labels: Vec<LabelRule>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            interval_secs: 15,
            retention_hours: 24,
            change_retention_days: 30,
            listen: "127.0.0.1:7171".to_string(),
            allowed_hosts: Vec::new(),
            labels: Vec::new(),
        }
    }
}

impl Config {
    /// Load `path`, or the default location if it exists, or built-in defaults.
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let (path, required) = match path {
            Some(p) => (p.to_path_buf(), true),
            None => (default_config_path(), false),
        };
        if !path.exists() {
            anyhow::ensure!(!required, "config file {} not found", path.display());
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }
}

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn xdg(var: &str, fallback: &str) -> PathBuf {
    std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .unwrap_or_else(|| home().join(fallback))
}

pub fn default_config_path() -> PathBuf {
    xdg("XDG_CONFIG_HOME", ".config").join("pulseblade/pulseblade.toml")
}

pub fn default_db_path() -> PathBuf {
    xdg("XDG_STATE_HOME", ".local/state").join("pulseblade/pulseblade.db")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_readme_example() {
        let cfg: Config = toml::from_str(
            r#"
            interval_secs = 15
            retention_hours = 24

            [[labels]]
            match = "unit:*:sshd.service"
            set = { criticality = "high", role = "access" }
            "#,
        )
        .unwrap();
        assert_eq!(cfg.labels.len(), 1);
        assert_eq!(cfg.labels[0].set["role"], "access");
        assert_eq!(cfg.listen, "127.0.0.1:7171");
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(toml::from_str::<Config>("intervl_secs = 5").is_err());
    }
}
