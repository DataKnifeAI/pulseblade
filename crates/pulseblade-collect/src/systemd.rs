use std::collections::HashMap;
use std::process::Command;

use pulseblade_core::{Health, Observation, Resource, ResourceKind};
use serde::Deserialize;
use serde_json::json;

use crate::{run_command, CollectError, Collector};

/// System units and user units share a parser; the scope only changes ids and parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnitScope {
    System,
    User,
}

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
        let units = systemctl(&["list-units", "--all", "--type=service", "--output=json"])?;
        let files = systemctl(&["list-unit-files", "--type=service", "--output=json"])?;
        let mut resources = parse_units(&self.host, UnitScope::System, &units, &files)?;
        if let Ok(show) = systemctl(&[
            "show",
            "--type=service",
            "--property=Id",
            "--property=Requires",
        ]) {
            attach_requires(
                &mut resources,
                &parse_requires(&String::from_utf8_lossy(&show)),
                |unit| format!("unit:{}:{unit}", self.host),
            );
        }
        Ok(Observation {
            resources,
            samples: Vec::new(),
            logs: Vec::new(),
        })
    }
}

pub(crate) fn systemctl(args: &[&str]) -> Result<Vec<u8>, CollectError> {
    run_command("systemctl", args)
}

pub(crate) fn parse_units(
    host: &str,
    scope: UnitScope,
    units: &[u8],
    files: &[u8],
) -> Result<Vec<Resource>, CollectError> {
    let units: Vec<UnitRow> = serde_json::from_slice(units)?;
    let files: Vec<UnitFileRow> = serde_json::from_slice(files)?;
    let file_state: HashMap<String, String> =
        files.into_iter().map(|f| (f.unit_file, f.state)).collect();
    let (parent, scope_label) = match scope {
        UnitScope::System => (format!("host:{host}"), "system"),
        UnitScope::User => (format!("session:{host}"), "user"),
    };

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
                unit_id(host, scope, &u.unit),
                ResourceKind::Service,
                &u.unit,
            )
            .parent(&parent)
            .health(unit_health(&u.active))
            .label("host", host)
            .label("scope", scope_label)
            .attr("active", u.active)
            .attr("sub", u.sub)
            .attr("unit_file_state", enabled)
            .attr("description", u.description)
        })
        .collect())
}

pub(crate) fn unit_id(host: &str, scope: UnitScope, unit: &str) -> String {
    match scope {
        UnitScope::System => format!("unit:{host}:{unit}"),
        UnitScope::User => format!("unit:{host}:user:{unit}"),
    }
}

/// `systemctl show` text: unit name to the `.service` units it requires.
pub(crate) fn parse_requires(text: &str) -> HashMap<String, Vec<String>> {
    let mut out = HashMap::new();
    let mut id = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            id = None;
            continue;
        }
        if let Some(v) = line.strip_prefix("Id=") {
            id = Some(v.to_string());
        } else if let Some(v) = line.strip_prefix("Requires=") {
            if let Some(id) = &id {
                let deps: Vec<String> = v
                    .split_whitespace()
                    .filter(|d| d.ends_with(".service") && d != id)
                    .take(8)
                    .map(str::to_string)
                    .collect();
                if !deps.is_empty() {
                    out.insert(id.clone(), deps);
                }
            }
        }
    }
    out
}

pub(crate) fn attach_requires(
    resources: &mut [Resource],
    requires: &HashMap<String, Vec<String>>,
    id_for: impl Fn(&str) -> String,
) {
    for resource in resources {
        let Some(deps) = requires.get(&resource.name) else {
            continue;
        };
        let ids: Vec<String> = deps.iter().map(|d| id_for(d)).collect();
        if !ids.is_empty() {
            resource.attrs.insert("requires".into(), json!(ids));
        }
    }
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
        let rs = parse_units("h", UnitScope::System, UNITS.as_bytes(), FILES.as_bytes()).unwrap();
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
        assert_eq!(sshd.labels["scope"], "system");
    }

    #[test]
    fn user_units_are_namespaced() {
        let rs = parse_units("h", UnitScope::User, UNITS.as_bytes(), FILES.as_bytes()).unwrap();
        let sshd = rs.iter().find(|r| r.name == "sshd.service").unwrap();
        assert_eq!(sshd.id, "unit:h:user:sshd.service");
        assert_eq!(sshd.parent.as_deref(), Some("session:h"));
        assert_eq!(sshd.labels["scope"], "user");
    }

    #[test]
    fn requires_become_resource_ids() {
        let text = "\
Id=sshd.service
Requires=system.slice foo.service bar.service sshd.service

Id=foo.service
Requires=bar.service
";
        let map = parse_requires(text);
        assert_eq!(map["sshd.service"], ["foo.service", "bar.service"]);
        let mut rs =
            parse_units("h", UnitScope::System, UNITS.as_bytes(), FILES.as_bytes()).unwrap();
        attach_requires(&mut rs, &map, |unit| format!("unit:h:{unit}"));
        let sshd = rs.iter().find(|r| r.name == "sshd.service").unwrap();
        assert_eq!(
            sshd.attrs["requires"],
            serde_json::json!(["unit:h:foo.service", "unit:h:bar.service"])
        );
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
