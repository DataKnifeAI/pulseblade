//! Power actions for iDRAC and the rack PDU.
//!
//! `idrac.power_cycle` and `pdu.outlet_on` wait for approval. `pdu.outlet_off`
//! is denied, so a plan can name it and the node will not run it. The URL
//! comes from the configured target, never from the action parameters.

use crate::{ActError, SecretLookup};

/// One BMC or PDU the node is allowed to act on.
#[derive(Debug, Clone)]
pub struct HwEndpoint {
    pub name: String,
    pub url: String,
    pub user_env: String,
    pub secret_env: String,
    pub verify_tls: bool,
    /// `idrac` or `pdu`.
    pub kind: &'static str,
}

pub trait HwApi: Send + Sync {
    fn post_json(
        &self,
        url: &str,
        authorization: &str,
        body: &str,
        verify_tls: bool,
    ) -> Result<String, String>;
}

pub struct LiveHw;

impl HwApi for LiveHw {
    fn post_json(
        &self,
        url: &str,
        authorization: &str,
        body: &str,
        verify_tls: bool,
    ) -> Result<String, String> {
        let agent = agent(verify_tls)?;
        let response = agent
            .post(url)
            .set("Authorization", authorization)
            .set("Content-Type", "application/json")
            .set("Accept", "application/json")
            .send_string(body)
            .map_err(|e| clip(&e.to_string()))?;
        response
            .into_string()
            .map_err(|e| e.to_string())
            .map(|s| clip(&s))
    }
}

fn agent(verify_tls: bool) -> Result<ureq::Agent, String> {
    if verify_tls {
        return Ok(ureq::Agent::new());
    }
    let tls = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .map_err(|e| e.to_string())?;
    Ok(ureq::AgentBuilder::new()
        .tls_connector(std::sync::Arc::new(tls))
        .build())
}

pub fn is_hw(name: &str) -> bool {
    matches!(
        name,
        "idrac.power_cycle" | "pdu.outlet_on" | "pdu.outlet_off"
    )
}

pub fn description(name: &str) -> &'static str {
    match name {
        "idrac.power_cycle" => "Force-restart the host through its iDRAC.",
        "pdu.outlet_on" => "Turn one PDU outlet on.",
        "pdu.outlet_off" => "Turn one PDU outlet off. Denied unless the tier is changed.",
        _ => "",
    }
}

pub fn preconditions(name: &str) -> &'static [&'static str] {
    match name {
        "idrac.power_cycle" => &[
            "name is a configured iDRAC",
            "the reset is ForceRestart of System.Embedded.1",
        ],
        "pdu.outlet_on" => &[
            "name is a configured PDU",
            "outlet is one outlet id",
            "the call sets ResetType On",
        ],
        "pdu.outlet_off" => &[
            "name is a configured PDU",
            "the default tier is deny, so this does not run",
        ],
        _ => &[],
    }
}

pub fn schema(name: &str) -> serde_json::Value {
    match name {
        "idrac.power_cycle" => serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["name"],
            "properties": {
                "name": {"type": "string", "minLength": 1, "maxLength": 64}
            }
        }),
        "pdu.outlet_on" | "pdu.outlet_off" => serde_json::json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["name", "outlet"],
            "properties": {
                "name": {"type": "string", "minLength": 1, "maxLength": 64},
                "outlet": {"type": "string", "minLength": 1, "maxLength": 8},
                "rack": {"type": "string", "minLength": 1, "maxLength": 32}
            }
        }),
        _ => serde_json::json!({}),
    }
}

pub fn resource_id(name: &str, params: &serde_json::Value) -> Result<String, ActError> {
    match name {
        "idrac.power_cycle" => {
            let target = token(params, "name")?;
            Ok(format!("chassis:{target}"))
        }
        "pdu.outlet_on" | "pdu.outlet_off" => {
            let target = token(params, "name")?;
            let outlet = token(params, "outlet")?;
            Ok(format!("outlet:{target}:{outlet}"))
        }
        other => Err(ActError::Invalid(format!("unknown action `{other}`"))),
    }
}

pub fn ensure_known(
    name: &str,
    params: &serde_json::Value,
    targets: &[HwEndpoint],
) -> Result<(), ActError> {
    let kind = match name {
        "idrac.power_cycle" => "idrac",
        "pdu.outlet_on" | "pdu.outlet_off" => "pdu",
        other => return Err(ActError::Invalid(format!("unknown action `{other}`"))),
    };
    let target = token(params, "name")?;
    if !targets
        .iter()
        .any(|row| row.kind == kind && row.name == target && !row.url.trim().is_empty())
    {
        return Err(ActError::Invalid(format!(
            "{kind} `{target}` is not configured"
        )));
    }
    if kind == "pdu" {
        let _ = token(params, "outlet")?;
        if let Some(rack) = params.get("rack").and_then(|v| v.as_str()) {
            token_str(rack, "rack")?;
        }
    }
    Ok(())
}

pub fn perform(
    name: &str,
    params: &serde_json::Value,
    targets: &[HwEndpoint],
    api: &dyn HwApi,
    secrets: &dyn SecretLookup,
) -> Result<String, String> {
    let kind = match name {
        "idrac.power_cycle" => "idrac",
        "pdu.outlet_on" | "pdu.outlet_off" => "pdu",
        other => return Err(format!("unknown action `{other}`")),
    };
    let target_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let target = targets
        .iter()
        .find(|row| row.kind == kind && row.name == target_name)
        .ok_or_else(|| format!("{kind} `{target_name}` is not configured"))?;
    let user = secrets
        .get(&target.user_env)
        .ok_or_else(|| format!("environment variable {} is unset", target.user_env))?;
    let pass = secrets
        .get(&target.secret_env)
        .ok_or_else(|| format!("environment variable {} is unset", target.secret_env))?;
    let auth = basic(&user, &pass);
    let base = target.url.trim().trim_end_matches('/');
    let (url, body) = match name {
        "idrac.power_cycle" => (
            format!("{base}/redfish/v1/Systems/System.Embedded.1/Actions/ComputerSystem.Reset"),
            r#"{"ResetType":"ForceRestart"}"#,
        ),
        "pdu.outlet_on" | "pdu.outlet_off" => {
            let outlet = params.get("outlet").and_then(|v| v.as_str()).unwrap_or("");
            let rack = params
                .get("rack")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("1");
            let reset = if name == "pdu.outlet_on" {
                "On"
            } else {
                "ForceOff"
            };
            (
                format!(
                    "{base}/redfish/v1/PowerEquipment/RackPDUs/{rack}/Outlets/{outlet}/Actions/Outlet.PowerControl"
                ),
                if reset == "On" {
                    r#"{"ResetType":"On"}"#
                } else {
                    r#"{"ResetType":"ForceOff"}"#
                },
            )
        }
        other => return Err(format!("unknown action `{other}`")),
    };
    api.post_json(&url, &auth, body, target.verify_tls)?;
    Ok(match name {
        "idrac.power_cycle" => format!("power cycled {target_name}"),
        "pdu.outlet_on" => format!(
            "turned on outlet {} of {target_name}",
            params.get("outlet").and_then(|v| v.as_str()).unwrap_or("")
        ),
        "pdu.outlet_off" => format!(
            "turned off outlet {} of {target_name}",
            params.get("outlet").and_then(|v| v.as_str()).unwrap_or("")
        ),
        _ => "ok".into(),
    })
}

fn token(params: &serde_json::Value, key: &str) -> Result<String, ActError> {
    token_str(params.get(key).and_then(|v| v.as_str()).unwrap_or(""), key)
}

fn token_str(raw: &str, key: &str) -> Result<String, ActError> {
    let value = raw.trim();
    if value.is_empty() || value.len() > 64 || value.contains(['/', ' ', ':', '\\', '\n', '\0']) {
        return Err(ActError::Invalid(format!("{key} is not a single token")));
    }
    Ok(value.to_string())
}

fn basic(user: &str, pass: &str) -> String {
    use base64::Engine;
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"))
    )
}

fn clip(s: &str) -> String {
    if s.chars().count() <= 500 {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(500).collect();
        out.push('…');
        out
    }
}
