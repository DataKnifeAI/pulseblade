//! User systemd units (`systemctl --user`) and the user session.
//!
//! Registered only when the user bus answers. A later pass that cannot reach
//! the bus returns an error so previously seen units are left in place.

use std::process::Command;

use pulseblade_core::{Health, Observation, Resource, ResourceKind};

use crate::systemd::{attach_requires, parse_requires, parse_units, systemctl, unit_id, UnitScope};
use crate::{CollectError, Collector};

pub struct UserCollector {
    host: String,
}

impl UserCollector {
    pub fn new(host: String) -> Self {
        Self { host }
    }

    /// True when this process can talk to its user systemd.
    pub fn available() -> bool {
        Command::new("systemctl")
            .args([
                "--user",
                "list-units",
                "--type=service",
                "--no-pager",
                "--plain",
            ])
            .output()
            .is_ok_and(|o| o.status.success())
    }
}

impl Collector for UserCollector {
    fn name(&self) -> &'static str {
        "user-systemd"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let units = systemctl(&[
            "--user",
            "list-units",
            "--all",
            "--type=service",
            "--output=json",
        ])?;
        let files = systemctl(&[
            "--user",
            "list-unit-files",
            "--type=service",
            "--output=json",
        ])?;
        let mut resources = parse_units(&self.host, UnitScope::User, &units, &files)?;
        if let Ok(show) = systemctl(&[
            "--user",
            "show",
            "--type=service",
            "--property=Id",
            "--property=Requires",
        ]) {
            let host = self.host.clone();
            attach_requires(
                &mut resources,
                &parse_requires(&String::from_utf8_lossy(&show)),
                |unit| unit_id(&host, UnitScope::User, unit),
            );
        }
        resources.insert(0, session_resource(&self.host));
        Ok(Observation {
            resources,
            samples: Vec::new(),
            logs: Vec::new(),
        })
    }
}

fn session_resource(host: &str) -> Resource {
    let user = command_text("id", &["-un"]).unwrap_or_else(|| "user".into());
    let uid = command_text("id", &["-u"]).unwrap_or_else(|| "0".into());
    let props = loginctl_user(&uid);
    let state = props
        .get("State")
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    let linger = props.get("Linger").cloned().unwrap_or_else(|| "no".into());
    let health = if state == "active" || linger == "yes" {
        Health::Ok
    } else if state == "closing" || state == "closing-force" {
        Health::Degraded
    } else {
        Health::Unknown
    };
    Resource::new(format!("session:{host}"), ResourceKind::Session, &user)
        .parent(format!("host:{host}"))
        .health(health)
        .label("host", host)
        .label("scope", "user")
        .attr("user", user)
        .attr("uid", uid)
        .attr("state", state)
        .attr("linger", linger)
}

fn loginctl_user(uid: &str) -> std::collections::BTreeMap<String, String> {
    let Ok(out) = Command::new("loginctl")
        .args([
            "show-user",
            uid,
            "-p",
            "State",
            "-p",
            "Linger",
            "-p",
            "Name",
            "--no-pager",
        ])
        .output()
    else {
        return std::collections::BTreeMap::new();
    };
    if !out.status.success() {
        return std::collections::BTreeMap::new();
    }
    parse_key_values(&String::from_utf8_lossy(&out.stdout))
}

fn parse_key_values(text: &str) -> std::collections::BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

fn command_text(bin: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(bin).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_values() {
        let props = parse_key_values("State=active\nLinger=yes\nName=ada\n");
        assert_eq!(props["State"], "active");
        assert_eq!(props["Name"], "ada");
    }
}
