//! Remedy shapes. A row remembers the action that cleared a finding, with
//! resource names turned into fields of the current resource.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

/// Resource kind encoded in an id (`workload:…` -> `workload`).
pub fn resource_kind_of_id(id: &str) -> Option<&'static str> {
    match id.split(':').next()? {
        "host" => Some("host"),
        "disk" => Some("disk"),
        "net" => Some("net_iface"),
        "unit" => Some("service"),
        "ctr" | "lxc" => Some("container"),
        "img" => Some("image"),
        "vm" => Some("vm"),
        "node" | "knode" => Some("node"),
        "pod" => Some("pod"),
        "proc" => Some("process"),
        "session" => Some("session"),
        "storage" => Some("storage"),
        "workload" => Some("workload"),
        _ => None,
    }
}

/// Named pieces of a resource id. `kind` on a workload is the Kubernetes kind
/// (`deployment`), not the resource kind (`workload`).
pub fn resource_fields(id: &str) -> BTreeMap<String, String> {
    let mut parts = id.split(':');
    let prefix = parts.next().unwrap_or("");
    let mut fields = BTreeMap::new();
    let put = |fields: &mut BTreeMap<String, String>, key: &str, value: &str| {
        if !value.is_empty() {
            fields.insert(key.to_string(), value.to_string());
        }
    };
    match prefix {
        "workload" => {
            let cluster = parts.next().unwrap_or("");
            let namespace = parts.next().unwrap_or("");
            let kind = parts.next().unwrap_or("");
            let name = parts.collect::<Vec<_>>().join(":");
            put(&mut fields, "cluster", cluster);
            put(&mut fields, "namespace", namespace);
            put(&mut fields, "kind", kind);
            put(&mut fields, "name", &name);
        }
        "pod" => {
            let cluster = parts.next().unwrap_or("");
            let namespace = parts.next().unwrap_or("");
            let name = parts.collect::<Vec<_>>().join(":");
            put(&mut fields, "cluster", cluster);
            put(&mut fields, "namespace", namespace);
            put(&mut fields, "name", &name);
        }
        "unit" => {
            let host = parts.next().unwrap_or("");
            let rest: Vec<&str> = parts.collect();
            put(&mut fields, "host", host);
            if rest.first().copied() == Some("user") && rest.len() >= 2 {
                put(&mut fields, "unit", &rest[1..].join(":"));
            } else {
                put(&mut fields, "unit", &rest.join(":"));
            }
        }
        "ctr" => {
            let host = parts.next().unwrap_or("");
            let name = parts.collect::<Vec<_>>().join(":");
            put(&mut fields, "host", host);
            put(&mut fields, "name", &name);
            put(&mut fields, "container", &name);
        }
        "disk" => {
            let host = parts.next().unwrap_or("");
            let mount = parts.collect::<Vec<_>>().join(":");
            put(&mut fields, "host", host);
            put(&mut fields, "mount", &mount);
            put(&mut fields, "path", &mount);
        }
        "vm" | "lxc" => {
            let node = parts.next().unwrap_or("");
            let vmid = parts.collect::<Vec<_>>().join(":");
            put(&mut fields, "node", node);
            put(&mut fields, "vmid", &vmid);
        }
        "host" => {
            let host = parts.collect::<Vec<_>>().join(":");
            put(&mut fields, "host", &host);
            put(&mut fields, "name", &host);
        }
        _ => {
            if let Some(host) = crate::host_of_id(id) {
                put(&mut fields, "host", host);
            }
            if let Some(name) = id.rsplit(':').next() {
                put(&mut fields, "name", name);
            }
        }
    }
    fields
}

/// Replace resource identity with `$field` tokens. A rollout restart keeps
/// `kind`, `namespace`, and `name` as fields of whatever resource is failing
/// now, not the deployment name that was restarted last time.
pub fn generalize_params(action: &str, params: &Value, resource_id: &str) -> Value {
    let mut shape = if params.is_object() {
        params.clone()
    } else {
        Value::Object(Map::new())
    };
    let Some(obj) = shape.as_object_mut() else {
        return shape;
    };
    let fields = resource_fields(resource_id);
    for value in obj.values_mut() {
        let Some(text) = value.as_str() else {
            continue;
        };
        if let Some(field) = fields
            .iter()
            .find(|(_, have)| have.as_str() == text)
            .map(|(field, _)| field.clone())
        {
            *value = Value::String(format!("${field}"));
        }
    }
    match action {
        "k8s.rollout_restart" | "k8s.scale" => {
            obj.insert("namespace".into(), Value::String("$namespace".into()));
            obj.insert("name".into(), Value::String("$name".into()));
            if action == "k8s.rollout_restart" || obj.contains_key("kind") {
                obj.insert("kind".into(), Value::String("$kind".into()));
            }
        }
        "systemd.restart" => {
            obj.insert("unit".into(), Value::String("$unit".into()));
        }
        "docker.restart" => {
            obj.insert("container".into(), Value::String("$container".into()));
        }
        "proxmox.start" | "proxmox.stop" | "proxmox.migrate" => {
            obj.insert("node".into(), Value::String("$node".into()));
            obj.insert("vmid".into(), Value::String("$vmid".into()));
        }
        _ => {}
    }
    shape
}

/// Fill `$field` tokens from `resource_id`. `None` when a token has no field.
pub fn bind_remedy_params(shape: &Value, resource_id: &str) -> Option<Value> {
    let fields = resource_fields(resource_id);
    let obj = shape.as_object()?;
    let mut out = Map::new();
    for (key, value) in obj {
        if let Some(token) = value.as_str().and_then(|text| text.strip_prefix('$')) {
            if token.is_empty() || token.contains(|c: char| c.is_whitespace()) {
                out.insert(key.clone(), value.clone());
                continue;
            }
            let filled = fields.get(token)?.clone();
            out.insert(key.clone(), Value::String(filled));
        } else {
            out.insert(key.clone(), value.clone());
        }
    }
    Some(Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rollout_restart_uses_the_current_workload() {
        let stored =
            json!({"namespace": "pb-fault", "name": "pb-fault-loop", "kind": "deployment"});
        let shape = generalize_params(
            "k8s.rollout_restart",
            &stored,
            "workload:poc-apps:pb-fault:deployment:pb-fault-loop",
        );
        assert_eq!(shape["namespace"], "$namespace");
        assert_eq!(shape["name"], "$name");
        assert_eq!(shape["kind"], "$kind");
        assert!(!shape.to_string().contains("pb-fault-loop"));
        let bound = bind_remedy_params(&shape, "workload:poc-apps:other:deployment:api").unwrap();
        assert_eq!(bound["namespace"], "other");
        assert_eq!(bound["name"], "api");
        assert_eq!(bound["kind"], "deployment");
    }
}
