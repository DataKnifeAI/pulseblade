//! Scrape Prometheus text exposition into samples.
//!
//! Each target becomes `prom:<host>:<name>`. A target that cannot be scraped
//! is recorded failed; the node keeps running. Pulseblade's own `/metrics`
//! is the other half of the interop and is served by the HTTP API.

use std::collections::BTreeMap;
#[cfg(all(test, any(feature = "k8s", feature = "proxmox")))]
use std::io::{Read, Write};
#[cfg(all(test, any(feature = "k8s", feature = "proxmox")))]
use std::net::TcpListener;

use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};

use crate::{CollectError, Collector};

const MAX_SERIES: usize = 40;

#[derive(Debug, Clone)]
pub struct PromTarget {
    pub name: String,
    pub url: String,
}

pub trait PromSource: Send {
    fn scrape(&mut self, url: &str) -> Result<Vec<PromSeries>, CollectError>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromSeries {
    pub name: String,
    pub value: f64,
}

pub struct PromCollector<S> {
    host: String,
    targets: Vec<PromTarget>,
    source: S,
}

impl<S> PromCollector<S> {
    pub fn new(host: impl Into<String>, targets: Vec<PromTarget>, source: S) -> Self {
        Self {
            host: host.into(),
            targets,
            source,
        }
    }
}

impl<S: PromSource> Collector for PromCollector<S> {
    fn name(&self) -> &'static str {
        "prometheus"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let mut obs = Observation::default();
        for target in &self.targets {
            let Some(name) = id_part(&target.name) else {
                continue;
            };
            match self.source.scrape(&target.url) {
                Ok(series) => {
                    let part = observe_prom(&self.host, name, &series);
                    obs.resources.extend(part.resources);
                    obs.samples.extend(part.samples);
                }
                Err(e) => {
                    let id = format!("prom:{}:{name}", self.host);
                    obs.resources.push(
                        Resource::new(&id, ResourceKind::Service, name)
                            .parent(format!("host:{}", self.host))
                            .health(Health::Failed)
                            .label("host", &self.host)
                            .label("role", "metrics")
                            .attr("error", clip(&e.to_string())),
                    );
                }
            }
        }
        Ok(obs)
    }
}

fn id_part(raw: &str) -> Option<&str> {
    let s = raw.trim();
    if s.is_empty() || s.contains(':') || s.contains('/') {
        None
    } else {
        Some(s)
    }
}

fn clip(s: &str) -> String {
    if s.chars().count() <= 200 {
        s.to_string()
    } else {
        s.chars().take(200).collect()
    }
}

pub fn observe_prom(host: &str, target: &str, series: &[PromSeries]) -> Observation {
    let id = format!("prom:{host}:{target}");
    let mut obs = Observation::default();
    obs.resources.push(
        Resource::new(&id, ResourceKind::Service, target)
            .parent(format!("host:{host}"))
            .health(Health::Ok)
            .label("host", host)
            .label("role", "metrics")
            .attr("job", target),
    );
    for sample in series
        .iter()
        .filter(|s| s.value.is_finite())
        .take(MAX_SERIES)
    {
        let metric = format!("prom.{}", sanitize(&sample.name));
        obs.samples.push(Sample::new(&id, metric, sample.value));
    }
    obs
}

/// Parse a Prometheus text exposition. Histograms and comments are skipped.
/// The first sample of each metric name is kept.
pub fn parse_exposition(text: &str) -> Vec<PromSeries> {
    let mut out = Vec::new();
    let mut seen = BTreeMap::<String, ()>::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = split_sample(line) else {
            continue;
        };
        if name.ends_with("_bucket") || name.ends_with("_sum") || name.ends_with("_count") {
            continue;
        }
        if seen.insert(name.clone(), ()).is_some() {
            continue;
        }
        out.push(PromSeries { name, value });
        if out.len() >= MAX_SERIES {
            break;
        }
    }
    out
}

fn split_sample(line: &str) -> Option<(String, f64)> {
    let (lhs, rhs) = line.rsplit_once(|c: char| c.is_whitespace())?;
    let value: f64 = rhs.parse().ok()?;
    let name = lhs.split('{').next()?.trim();
    if name.is_empty() {
        return None;
    }
    Some((name.to_string(), value))
}

fn sanitize(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    out
}

#[cfg(any(feature = "k8s", feature = "proxmox"))]
pub struct LiveProm {
    agent: ureq::Agent,
}

#[cfg(any(feature = "k8s", feature = "proxmox"))]
impl LiveProm {
    pub fn new() -> Self {
        Self {
            agent: ureq::AgentBuilder::new()
                .timeout(std::time::Duration::from_secs(5))
                .build(),
        }
    }
}

#[cfg(any(feature = "k8s", feature = "proxmox"))]
impl Default for LiveProm {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(feature = "k8s", feature = "proxmox"))]
impl PromSource for LiveProm {
    fn scrape(&mut self, url: &str) -> Result<Vec<PromSeries>, CollectError> {
        let resp = self
            .agent
            .get(url)
            .call()
            .map_err(|e| CollectError::Unavailable(format!("prometheus scrape: {e}")))?;
        let text = resp
            .into_string()
            .map_err(|e| CollectError::Unavailable(e.to_string()))?;
        Ok(parse_exposition(&text))
    }
}

#[cfg(all(test, any(feature = "k8s", feature = "proxmox")))]
pub fn serve_metrics(body: &'static str) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes());
        }
    });
    (format!("http://{addr}/metrics"), handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_small_exposition() {
        let text = "\
# HELP up target is up
# TYPE up gauge
up 1
http_requests_total{code=\"200\"} 12
http_request_duration_seconds_bucket{le=\"0.1\"} 3
http_request_duration_seconds_sum 1.5
";
        let series = parse_exposition(text);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].name, "up");
        assert_eq!(series[1].value, 12.0);
        let obs = observe_prom("web1", "app", &series);
        assert_eq!(obs.resources[0].id, "prom:web1:app");
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "prom.up" && s.value == 1.0));
    }

    #[cfg(any(feature = "k8s", feature = "proxmox"))]
    #[test]
    fn fixture_metrics_endpoint_is_scraped() {
        let (url, handle) = serve_metrics("up 1\nqueue_depth 4\n");
        let mut collector = PromCollector::new(
            "web1",
            vec![PromTarget {
                name: "app".into(),
                url,
            }],
            LiveProm::new(),
        );
        let obs = collector.collect().unwrap();
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "prom.queue_depth" && s.value == 4.0));
        let _ = handle.join();
    }
}
