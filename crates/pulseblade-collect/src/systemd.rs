use std::collections::HashMap;
use std::process::Command;

use pulseblade_core::{Health, Observation, Resource, ResourceKind};
use serde::Deserialize;

use crate::{CollectError, Collector};

/// System-level systemd services via `systemctl --output=json`.
pub struct SystemdCollector {
    host: String,
}

#[derive(Debug, Deserialize)]
struct UnitRow {
    unit: String,
    load: String,
    active: String,
    sub: String,
    #[serde(default)]
    description: String,
}

#[derive(Debug, Deserialize)]
struct UnitFileRow {
    unit_file: String,
    state: String,
}

impl SystemdCollector {
    pub fn new(host: String) -> Self {
        Self { host }
    }

    pub fn available() -> bool {
        Command::new("systemctl")
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
    }
}

impl Collector for SystemdCollector {
    fn name(&self) -> &'static str {
        "systemd"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let units = systemctl(&["list-units", "--all", "--type=service"])?;
        let files = systemctl(&["list-unit-files", "--type=service"])?;
        Ok(Observation {
            resources: parse_units(&self.host, &units, &files)?,
            samples: Vec::new(),
        })
    }
}

fn systemctl(args: &[&str]) -> Result<Vec<u8>, CollectError> {
    let out = Command::new("systemctl")
        .args(args)
        .args(["--output=json", "--no-pager", "--plain"])
        .output()?;
    if !out.status.success() {
        return Err(CollectError::Command(
            "systemctl",
            String::from_utf8_lossy(&out.stderr).trim().to_string(),
        ));
    }
    Ok(out.stdout)
}

fn parse_units(host: &str, units: &[u8], files: &[u8]) -> Result<Vec<Resource>, CollectError> {
    let units: Vec<UnitRow> = serde_json::from_slice(units)?;
    let files: Vec<UnitFileRow> = serde_json::from_slice(files)?;
    let file_state: HashMap<String, String> =
        files.into_iter().map(|f| (f.unit_file, f.state)).collect();
    let parent = format!("host:{host}");

    Ok(units
        .into_iter()
        .filter(|u| u.load == "loaded")
        .map(|u| {
            let enabled = file_state
                .get(&u.unit)
                .or_else(|| template_of(&u.unit).and_then(|t| file_state.get(&t)))
                .cloned()
                .unwrap_or_default();
            Resource::new(
                format!("unit:{host}:{}", u.unit),
                ResourceKind::Service,
                &u.unit,
            )
            .parent(&parent)
            .health(unit_health(&u.active))
            .label("host", host)
            .attr("active", u.active)
            .attr("sub", u.sub)
            .attr("unit_file_state", enabled)
            .attr("description", u.description)
        })
        .collect())
}

/// `getty@tty1.service` -> `getty@.service`, whose enablement state applies.
fn template_of(unit: &str) -> Option<String> {
    let (prefix, rest) = unit.split_once('@')?;
    let suffix = rest.rsplit_once('.')?.1;
    Some(format!("{prefix}@.{suffix}"))
}

fn unit_health(active: &str) -> Health {
    match active {
        "active" | "inactive" => Health::Ok,
        "failed" => Health::Failed,
        "activating" | "deactivating" | "reloading" | "refreshing" => Health::Degraded,
        _ => Health::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNITS: &str = r#"[
        {"unit":"sshd.service","load":"loaded","active":"active","sub":"running","description":"OpenSSH Daemon"},
        {"unit":"broken.service","load":"loaded","active":"failed","sub":"failed","description":"Broken"},
        {"unit":"getty@tty1.service","load":"loaded","active":"active","sub":"running","description":"Getty on tty1"},
        {"unit":"ghost.service","load":"not-found","active":"inactive","sub":"dead","description":"ghost.service"}
    ]"#;
    const FILES: &str = r#"[
        {"unit_file":"sshd.service","state":"enabled","preset":"disabled"},
        {"unit_file":"getty@.service","state":"enabled","preset":"enabled"}
    ]"#;

    #[test]
    fn parses_units() {
        let rs = parse_units("h", UNITS.as_bytes(), FILES.as_bytes()).unwrap();
        assert_eq!(rs.len(), 3, "not-found units are skipped");

        let sshd = rs.iter().find(|r| r.name == "sshd.service").unwrap();
        assert_eq!(sshd.id, "unit:h:sshd.service");
        assert_eq!(sshd.parent.as_deref(), Some("host:h"));
        assert_eq!(sshd.health, Health::Ok);
        assert_eq!(sshd.attrs["unit_file_state"], "enabled");

        let broken = rs.iter().find(|r| r.name == "broken.service").unwrap();
        assert_eq!(broken.health, Health::Failed);

        let getty = rs.iter().find(|r| r.name == "getty@tty1.service").unwrap();
        assert_eq!(getty.attrs["unit_file_state"], "enabled");
    }

    #[test]
    fn templates() {
        assert_eq!(
            template_of("getty@tty1.service").as_deref(),
            Some("getty@.service")
        );
        assert_eq!(template_of("sshd.service"), None);
    }
}
