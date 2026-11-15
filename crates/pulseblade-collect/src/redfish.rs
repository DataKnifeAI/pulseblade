//! iDRAC and APC rack PDUs over Redfish.
//!
//! Both speak HTTPS JSON. Basic auth uses environment variable names from
//! config. A `verify_tls` of false is explicit: the iDRAC certificates here
//! are expired, and the collector does not skip verification on its own.
//! Ids: `host:<name>`, `chassis:<name>`, `pdu:<name>`, `outlet:<name>:<id>`.

use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};
use serde_json::Value;

use crate::hw::{ident, num, round2, worse, HwLimits, HwTarget};
use crate::{CollectError, Collector};

#[derive(Debug, Clone, PartialEq)]
pub struct BmcTemp {
    pub name: String,
    pub celsius: f64,
    pub upper_warning: Option<f64>,
    pub upper_critical: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BmcView {
    pub name: String,
    pub model: String,
    pub power: String,
    pub health: String,
    pub temps: Vec<BmcTemp>,
    pub supply_failed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PduOutlet {
    pub id: String,
    pub name: String,
    pub power: String,
    pub health: String,
    pub watts: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PduView {
    pub name: String,
    pub health: String,
    pub watts: Option<f64>,
    pub rated_watts: f64,
    pub volts: Option<f64>,
    pub outlets: Vec<PduOutlet>,
    pub limits: HwLimits,
}

pub trait RedfishSource: Send {
    fn chassis(&mut self) -> Result<BmcView, CollectError>;
}

pub trait PduSource: Send {
    fn pdu(&mut self) -> Result<PduView, CollectError>;
}

pub struct ChassisCollector<S> {
    source: S,
}

impl<S> ChassisCollector<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }
}

impl<S: RedfishSource> Collector for ChassisCollector<S> {
    fn name(&self) -> &'static str {
        "idrac"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        Ok(observe_chassis(&self.source.chassis()?))
    }
}

pub struct PduCollector<S> {
    source: S,
}

impl<S> PduCollector<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }
}

impl<S: PduSource> Collector for PduCollector<S> {
    fn name(&self) -> &'static str {
        "pdu"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        Ok(observe_pdu(&self.source.pdu()?))
    }
}

pub fn observe_chassis(view: &BmcView) -> Observation {
    let Some(name) = ident(&view.name) else {
        return Observation::default();
    };
    let host_id = format!("host:{name}");
    let id = format!("chassis:{name}");
    let (health, fault) = chassis_health(view);
    let mut chassis = Resource::new(&id, ResourceKind::Chassis, name)
        .parent(host_id.clone())
        .health(health)
        .label("host", name)
        .attr("power", view.power.clone());
    if !view.model.is_empty() {
        chassis = chassis.attr("model", view.model.clone());
    }
    if view.supply_failed {
        chassis = chassis.attr("psu", "failed");
    }
    if let Some(fault) = &fault {
        chassis = chassis.attr("fault", fault.clone());
    }
    let mut host = Resource::new(&host_id, ResourceKind::Host, name)
        .health(health)
        .label("role", "bmc")
        .attr("role", "idrac")
        .attr("power", view.power.clone());
    if let Some(fault) = fault {
        host = host.attr("fault", fault);
    }
    let mut obs = Observation {
        resources: vec![host, chassis],
        ..Observation::default()
    };
    if let Some(temp) = view.temps.iter().map(|t| t.celsius).reduce(f64::max) {
        obs.samples
            .push(Sample::new(&id, "chassis.temp_c", round2(temp)));
    }
    obs
}

fn chassis_health(view: &BmcView) -> (Health, Option<String>) {
    let mut health = redfish_health(&view.health);
    let mut why: Vec<&str> = Vec::new();
    if health == Health::Failed {
        why.push("health critical");
    } else if health == Health::Degraded {
        why.push("health warning");
    }
    if view.supply_failed {
        health = Health::Failed;
        why.push("power supply");
    }
    for temp in &view.temps {
        let over_crit = temp
            .upper_critical
            .map(|limit| temp.celsius >= limit)
            .unwrap_or(temp.celsius >= 85.0);
        let over_warn = temp
            .upper_warning
            .map(|limit| temp.celsius >= limit)
            .unwrap_or(temp.celsius >= 75.0);
        if over_crit {
            health = Health::Failed;
            why.push("thermal critical");
        } else if over_warn {
            health = worse(health, Health::Degraded);
            why.push("thermal warning");
        }
    }
    if health == Health::Unknown {
        health = Health::Ok;
    }
    let fault = if why.is_empty() {
        None
    } else {
        Some(join_unique(&why))
    };
    (health, fault)
}

pub fn observe_pdu(view: &PduView) -> Observation {
    let Some(name) = ident(&view.name) else {
        return Observation::default();
    };
    let host_id = format!("host:{name}");
    let id = format!("pdu:{name}");
    let load_pct = load_pct(view.watts, view.rated_watts);
    let (health, fault) = pdu_health(view, load_pct);
    let mut pdu = Resource::new(&id, ResourceKind::Pdu, name)
        .parent(host_id.clone())
        .health(health)
        .label("host", name);
    if let Some(fault) = &fault {
        pdu = pdu.attr("fault", fault.clone());
    }
    let mut host = Resource::new(&host_id, ResourceKind::Host, name)
        .health(health)
        .label("role", "power")
        .attr("role", "pdu");
    if let Some(fault) = fault {
        host = host.attr("fault", fault);
    }
    let mut obs = Observation {
        resources: vec![host, pdu],
        ..Observation::default()
    };
    if let Some(watts) = view.watts {
        obs.samples
            .push(Sample::new(&id, "pdu.watts", round2(watts)));
    }
    if let Some(volts) = view.volts {
        obs.samples
            .push(Sample::new(&id, "pdu.volts", round2(volts)));
    }
    if let Some(pct) = load_pct {
        obs.samples
            .push(Sample::new(&id, "pdu.load_pct", round2(pct)));
    }
    for outlet in &view.outlets {
        let Some(outlet_id) = ident(&outlet.id) else {
            continue;
        };
        let oid = format!("outlet:{name}:{outlet_id}");
        let health = redfish_health(&outlet.health);
        let health = if health == Health::Unknown {
            Health::Ok
        } else {
            health
        };
        let label = if outlet.name.is_empty() {
            outlet_id
        } else {
            outlet.name.as_str()
        };
        let mut resource = Resource::new(&oid, ResourceKind::Outlet, label)
            .parent(id.clone())
            .health(health)
            .label("host", name)
            .attr("power", outlet.power.clone());
        if health.is_unhealthy() {
            resource = resource.attr("fault", "outlet health");
        }
        obs.resources.push(resource);
        if let Some(watts) = outlet.watts {
            obs.samples
                .push(Sample::new(&oid, "outlet.watts", round2(watts)));
        }
    }
    obs
}

fn pdu_health(view: &PduView, load_pct: Option<f64>) -> (Health, Option<String>) {
    let mut health = redfish_health(&view.health);
    let mut why: Vec<&str> = Vec::new();
    if health == Health::Failed {
        why.push("health critical");
    } else if health == Health::Degraded {
        why.push("health warning");
    }
    if view.volts.is_some_and(|v| v < 1.0) {
        health = Health::Failed;
        why.push("input lost");
    }
    if let Some(pct) = load_pct {
        if pct >= view.limits.pdu_load_failed {
            health = Health::Failed;
            why.push("load high");
        } else if pct >= view.limits.pdu_load_degraded {
            health = worse(health, Health::Degraded);
            why.push("load high");
        }
    }
    if health == Health::Unknown {
        health = Health::Ok;
    }
    let fault = if why.is_empty() {
        None
    } else {
        Some(join_unique(&why))
    };
    (health, fault)
}

fn load_pct(watts: Option<f64>, rated: f64) -> Option<f64> {
    let watts = watts?;
    if rated <= 0.0 {
        return None;
    }
    Some(watts / rated * 100.0)
}

fn redfish_health(raw: &str) -> Health {
    match raw.trim().to_ascii_lowercase().as_str() {
        "ok" => Health::Ok,
        "warning" => Health::Degraded,
        "critical" => Health::Failed,
        _ => Health::Unknown,
    }
}

fn join_unique(parts: &[&str]) -> String {
    let mut out = Vec::new();
    for part in parts {
        if !out.contains(part) {
            out.push(*part);
        }
    }
    out.join("; ")
}

pub fn parse_chassis(
    name: &str,
    system: &Value,
    thermal: Option<&Value>,
    power: Option<&Value>,
) -> BmcView {
    let status = system.get("Status").unwrap_or(&Value::Null);
    let health = status
        .get("HealthRollup")
        .or_else(|| status.get("Health"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let mut temps = Vec::new();
    if let Some(rows) = thermal
        .and_then(|v| v.get("Temperatures"))
        .and_then(|v| v.as_array())
    {
        for row in rows {
            let state = row
                .get("Status")
                .and_then(|s| s.get("State"))
                .and_then(|v| v.as_str())
                .unwrap_or("Enabled");
            if state.eq_ignore_ascii_case("absent") || state.eq_ignore_ascii_case("disabled") {
                continue;
            }
            let Some(celsius) = num(row.get("ReadingCelsius")) else {
                continue;
            };
            temps.push(BmcTemp {
                name: row
                    .get("Name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                celsius,
                upper_warning: num(row.get("UpperThresholdNonCritical")),
                upper_critical: num(row.get("UpperThresholdCritical")),
            });
        }
    }
    let supply_failed = power
        .and_then(|v| v.get("PowerSupplies"))
        .and_then(|v| v.as_array())
        .map(|rows| {
            rows.iter().any(|row| {
                row.get("Status")
                    .and_then(|s| s.get("Health"))
                    .and_then(|v| v.as_str())
                    .is_some_and(|h| h.eq_ignore_ascii_case("critical"))
            })
        })
        .unwrap_or(false);
    BmcView {
        name: name.to_string(),
        model: system
            .get("Model")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        power: system
            .get("PowerState")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        health,
        temps,
        supply_failed,
    }
}

pub fn parse_pdu(
    name: &str,
    pdu: &Value,
    mains: &[Value],
    metrics: Option<&Value>,
    outlets: &[Value],
    rated_watts: f64,
    limits: HwLimits,
) -> PduView {
    let status = pdu.get("Status").unwrap_or(&Value::Null);
    let health = status
        .get("HealthRollup")
        .or_else(|| status.get("Health"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let rated = if rated_watts > 0.0 {
        rated_watts
    } else {
        num(pdu.get("RatedWatts"))
            .or_else(|| num(pdu.get("PowerCapacityWatts")))
            .unwrap_or(0.0)
    };
    let watts = metrics
        .and_then(reading)
        .or_else(|| mains.iter().find_map(reading));
    let volts = mains.iter().filter_map(voltage).reduce(f64::min);
    let outlets = outlets
        .iter()
        .filter_map(|row| {
            let id = row
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if id.is_empty() {
                return None;
            }
            let health = row
                .get("Status")
                .and_then(|s| s.get("Health"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Some(PduOutlet {
                id,
                name: row
                    .get("Name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                power: row
                    .get("PowerState")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                health,
                watts: reading(row),
            })
        })
        .collect();
    PduView {
        name: name.to_string(),
        health,
        watts,
        rated_watts: rated,
        volts,
        outlets,
        limits,
    }
}

fn reading(v: &Value) -> Option<f64> {
    num(v.pointer("/PowerWatts/Reading")).or_else(|| num(v.get("PowerWatts")))
}

fn voltage(v: &Value) -> Option<f64> {
    num(v.pointer("/Voltage/Reading")).or_else(|| num(v.get("Voltage")))
}

#[cfg(feature = "hw")]
pub struct LiveChassis {
    target: HwTarget,
}

#[cfg(feature = "hw")]
impl LiveChassis {
    pub fn new(target: HwTarget) -> Self {
        Self { target }
    }
}

#[cfg(feature = "hw")]
impl RedfishSource for LiveChassis {
    fn chassis(&mut self) -> Result<BmcView, CollectError> {
        let (agent, auth) = client(&self.target)?;
        let base = self.target.url.trim().trim_end_matches('/');
        let system = get_json(
            &agent,
            &auth,
            &format!("{base}/redfish/v1/Systems/System.Embedded.1"),
        )?;
        let thermal = get_json(
            &agent,
            &auth,
            &format!("{base}/redfish/v1/Chassis/System.Embedded.1/Thermal"),
        )
        .ok();
        let power = get_json(
            &agent,
            &auth,
            &format!("{base}/redfish/v1/Chassis/System.Embedded.1/Power"),
        )
        .ok();
        Ok(parse_chassis(
            &self.target.name,
            &system,
            thermal.as_ref(),
            power.as_ref(),
        ))
    }
}

#[cfg(feature = "hw")]
pub struct LivePdu {
    target: HwTarget,
}

#[cfg(feature = "hw")]
impl LivePdu {
    pub fn new(target: HwTarget) -> Self {
        Self { target }
    }
}

#[cfg(feature = "hw")]
impl PduSource for LivePdu {
    fn pdu(&mut self) -> Result<PduView, CollectError> {
        let (agent, auth) = client(&self.target)?;
        let base = self.target.url.trim().trim_end_matches('/');
        let list = get_json(
            &agent,
            &auth,
            &format!("{base}/redfish/v1/PowerEquipment/RackPDUs"),
        )?;
        let member = odata_members(&list)
            .into_iter()
            .next()
            .ok_or_else(|| CollectError::Unavailable("PDU Redfish list is empty".into()))?;
        let pdu = get_json(&agent, &auth, &abs(base, &member))?;
        let mains = fetch_members(&agent, &auth, base, link(&pdu, "Mains"))?;
        let metrics = link(&pdu, "Metrics")
            .map(|path| get_json(&agent, &auth, &abs(base, &path)))
            .transpose()?;
        let outlets = fetch_members(&agent, &auth, base, link(&pdu, "Outlets"))?;
        Ok(parse_pdu(
            &self.target.name,
            &pdu,
            &mains,
            metrics.as_ref(),
            &outlets,
            self.target.rated_watts,
            self.target.limits,
        ))
    }
}

#[cfg(feature = "hw")]
fn client(target: &HwTarget) -> Result<(ureq::Agent, String), CollectError> {
    let user = std::env::var(&target.user_env)
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            CollectError::Unavailable(format!("environment variable {} is unset", target.user_env))
        })?;
    let pass = std::env::var(&target.secret_env)
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            CollectError::Unavailable(format!(
                "environment variable {} is unset",
                target.secret_env
            ))
        })?;
    let agent = crate::http_agent(target.verify_tls)
        .map_err(|e| CollectError::Unavailable(format!("TLS setup failed: {e}")))?;
    Ok((agent, basic_auth(&user, &pass)))
}

#[cfg(feature = "hw")]
fn basic_auth(user: &str, pass: &str) -> String {
    use base64::Engine;
    let raw = format!("{user}:{pass}");
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(raw.as_bytes())
    )
}

#[cfg(feature = "hw")]
fn get_json(agent: &ureq::Agent, auth: &str, url: &str) -> Result<Value, CollectError> {
    let response = agent
        .get(url)
        .set("Authorization", auth)
        .set("Accept", "application/json")
        .call()
        .map_err(|e| CollectError::Unavailable(format!("Redfish GET failed: {e}")))?;
    let text = response
        .into_string()
        .map_err(|e| CollectError::Unavailable(format!("Redfish body failed: {e}")))?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|e| CollectError::Unavailable(format!("Redfish JSON failed: {e}")))?;
    if value.pointer("/error/code").and_then(|v| v.as_i64()) == Some(401) {
        return Err(CollectError::Unavailable("Redfish unauthorized".into()));
    }
    Ok(value)
}

#[cfg(feature = "hw")]
fn link(body: &Value, name: &str) -> Option<String> {
    body.pointer(&format!("/Links/{name}/@odata.id"))
        .or_else(|| body.pointer(&format!("/{name}/@odata.id")))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

#[cfg(feature = "hw")]
fn odata_members(body: &Value) -> Vec<String> {
    body.get("Members")
        .and_then(|v| v.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    row.get("@odata.id")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(feature = "hw")]
fn fetch_members(
    agent: &ureq::Agent,
    auth: &str,
    base: &str,
    collection: Option<String>,
) -> Result<Vec<Value>, CollectError> {
    let Some(path) = collection else {
        return Ok(Vec::new());
    };
    let list = get_json(agent, auth, &abs(base, &path))?;
    if list.get("Members").is_none() {
        return Ok(vec![list]);
    }
    let mut out = Vec::new();
    for member in odata_members(&list).into_iter().take(48) {
        out.push(get_json(agent, auth, &abs(base, &member))?);
    }
    Ok(out)
}

#[cfg(feature = "hw")]
fn abs(base: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        path.to_string()
    } else if let Some(path) = path.strip_prefix('/') {
        format!("{base}/{path}")
    } else {
        format!("{base}/{path}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_critical_supply_and_a_hot_inlet_fail_the_chassis() {
        let view = parse_chassis(
            "idrac01",
            &serde_json::json!({
                "Model": "PowerEdge R640",
                "PowerState": "On",
                "Status": {"Health": "Warning", "HealthRollup": "Warning"}
            }),
            Some(&serde_json::json!({
                "Temperatures": [
                    {
                        "Name": "Inlet",
                        "ReadingCelsius": 48,
                        "UpperThresholdNonCritical": 42,
                        "UpperThresholdCritical": 47,
                        "Status": {"State": "Enabled"}
                    },
                    {"Name": "Absent", "ReadingCelsius": 90, "Status": {"State": "Absent"}}
                ]
            })),
            Some(&serde_json::json!({
                "PowerSupplies": [
                    {"Name": "PS1", "Status": {"Health": "Critical"}},
                    {"Name": "PS2", "Status": {"Health": "OK"}}
                ]
            })),
        );
        let obs = observe_chassis(&view);
        let chassis = obs
            .resources
            .iter()
            .find(|r| r.id == "chassis:idrac01")
            .unwrap();
        assert_eq!(chassis.health, Health::Failed);
        let fault = chassis.attrs["fault"].as_str().unwrap();
        assert!(fault.contains("thermal critical"));
        assert!(fault.contains("power supply"));
        assert_eq!(chassis.attrs["power"], "On");
        assert_eq!(obs.samples[0].metric, "chassis.temp_c");
        assert_eq!(obs.samples[0].value, 48.0);
    }

    #[test]
    fn a_lost_input_fails_and_an_off_outlet_does_not() {
        let view = parse_pdu(
            "rack",
            &serde_json::json!({"Status": {"Health": "OK"}}),
            &[serde_json::json!({"Voltage": {"Reading": 0}})],
            Some(&serde_json::json!({"PowerWatts": {"Reading": 1800}})),
            &[
                serde_json::json!({"Id": "1", "Name": "Outlet 1", "PowerState": "Off", "Status": {"Health": "OK"}}),
                serde_json::json!({"Id": "2", "PowerState": "On", "Status": {"Health": "Warning"}}),
            ],
            2000.0,
            HwLimits::default(),
        );
        let obs = observe_pdu(&view);
        let pdu = obs.resources.iter().find(|r| r.id == "pdu:rack").unwrap();
        assert_eq!(pdu.health, Health::Failed);
        assert!(pdu.attrs["fault"].as_str().unwrap().contains("input lost"));
        let off = obs
            .resources
            .iter()
            .find(|r| r.id == "outlet:rack:1")
            .unwrap();
        assert_eq!(off.health, Health::Ok);
        assert_eq!(off.attrs["power"], "Off");
        let warn = obs
            .resources
            .iter()
            .find(|r| r.id == "outlet:rack:2")
            .unwrap();
        assert_eq!(warn.health, Health::Degraded);
    }

    #[test]
    fn load_without_input_loss_uses_the_rated_cutoff() {
        let view = parse_pdu(
            "rack",
            &serde_json::json!({"Status": {"Health": "OK"}}),
            &[serde_json::json!({"Voltage": {"Reading": 208}})],
            Some(&serde_json::json!({"PowerWatts": {"Reading": 1700}})),
            &[],
            2000.0,
            HwLimits::default(),
        );
        let obs = observe_pdu(&view);
        assert_eq!(obs.resources[1].health, Health::Degraded);
        assert_eq!(
            obs.samples
                .iter()
                .find(|s| s.metric == "pdu.load_pct")
                .unwrap()
                .value,
            85.0
        );
    }
}
