//! Docker containers and images.
//!
//! Ids are `ctr:<host>:<name>` and `img:<host>:<name>`. The live client uses
//! bollard and is compiled only with the `docker` feature. A missing daemon
//! disables the collector; a later API error fails that pass and leaves the
//! node running.

use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};

use crate::{CollectError, Collector};

const MAX_CONTAINERS: usize = 80;
const MAX_IMAGES: usize = 40;

#[derive(Debug, Clone, PartialEq)]
pub struct ContainerSnap {
    pub name: String,
    pub image: String,
    pub state: String,
    pub health: Option<String>,
    pub restart_count: u64,
    pub cpu_pct: Option<f64>,
    pub mem_bytes: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImageSnap {
    pub name: String,
    pub id: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DockerView {
    pub containers: Vec<ContainerSnap>,
    pub images: Vec<ImageSnap>,
}

pub trait DockerSource: Send {
    fn snapshot(&mut self) -> Result<DockerView, CollectError>;
}

pub struct DockerCollector<S> {
    host: String,
    source: S,
}

impl<S> DockerCollector<S> {
    pub fn new(host: impl Into<String>, source: S) -> Self {
        Self {
            host: host.into(),
            source,
        }
    }
}

impl<S: DockerSource> Collector for DockerCollector<S> {
    fn name(&self) -> &'static str {
        "docker"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let view = self.source.snapshot()?;
        Ok(observe_docker(&self.host, &view))
    }
}

pub fn container_health(state: &str, health: Option<&str>, restarts: u64) -> Health {
    let state = state.to_ascii_lowercase();
    let health = health.unwrap_or("").to_ascii_lowercase();
    if health == "unhealthy" || state == "dead" {
        Health::Failed
    } else if state == "restarting" || health == "starting" || restarts >= 8 {
        Health::Degraded
    } else if state == "running" {
        Health::Ok
    } else {
        Health::Unknown
    }
}

/// Build the observation for one docker pass. Names that cannot be resource ids
/// are skipped.
pub fn observe_docker(host: &str, view: &DockerView) -> Observation {
    let parent = format!("host:{host}");
    let mut obs = Observation::default();
    for ctr in view.containers.iter().take(MAX_CONTAINERS) {
        let Some(name) = id_part(&ctr.name) else {
            continue;
        };
        let id = format!("ctr:{host}:{name}");
        let health = container_health(&ctr.state, ctr.health.as_deref(), ctr.restart_count);
        let mut resource = Resource::new(&id, ResourceKind::Container, &name)
            .parent(&parent)
            .health(health)
            .label("host", host)
            .attr("state", ctr.state.clone())
            .attr("restart_count", ctr.restart_count);
        if !ctr.image.is_empty() {
            resource = resource.attr("image", ctr.image.clone());
        }
        if let Some(h) = &ctr.health {
            if !h.is_empty() {
                resource = resource.attr("health", h.clone());
            }
        }
        obs.resources.push(resource);
        if let Some(cpu) = ctr.cpu_pct {
            obs.samples
                .push(Sample::new(&id, "ctr.cpu_pct", round2(cpu)));
        }
        if let Some(mem) = ctr.mem_bytes {
            obs.samples.push(Sample::new(&id, "ctr.mem_bytes", mem));
        }
    }
    for image in view.images.iter().take(MAX_IMAGES) {
        let Some(name) = id_part(&image.name) else {
            continue;
        };
        if name.starts_with("<none>") {
            continue;
        }
        let id = format!("img:{host}:{name}");
        let resource = Resource::new(&id, ResourceKind::Image, &name)
            .parent(&parent)
            .health(Health::Ok)
            .label("host", host)
            .attr("image_id", image.id.clone())
            .attr("size_bytes", image.size_bytes);
        obs.resources.push(resource);
    }
    obs
}

fn id_part(raw: &str) -> Option<String> {
    let s = raw.trim().trim_start_matches('/');
    if s.is_empty() || s.chars().any(|c| c.is_control() || c == ' ') {
        None
    } else {
        Some(s.to_string())
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// CPU percent from two Docker stats samples. `None` when the daemon has not
/// yet produced a delta.
#[cfg(any(test, feature = "docker"))]
pub fn cpu_percent(
    cpu_total: u64,
    cpu_prev: u64,
    system: u64,
    system_prev: u64,
    online_cpus: u64,
) -> Option<f64> {
    let cpu_delta = cpu_total.saturating_sub(cpu_prev) as f64;
    let sys_delta = system.saturating_sub(system_prev) as f64;
    if sys_delta <= 0.0 || cpu_delta < 0.0 {
        return None;
    }
    let cpus = online_cpus.max(1) as f64;
    Some((cpu_delta / sys_delta) * cpus * 100.0)
}

#[cfg(feature = "docker")]
mod live {
    use super::{ContainerSnap, DockerSource, DockerView, ImageSnap};
    use crate::CollectError;
    use bollard::query_parameters::{
        InspectContainerOptions, ListContainersOptionsBuilder, ListImagesOptionsBuilder,
        StatsOptionsBuilder,
    };
    use bollard::Docker;
    use futures_util::StreamExt;

    pub struct LiveDocker {
        docker: Docker,
        rt: tokio::runtime::Runtime,
    }

    impl LiveDocker {
        pub fn connect() -> Result<Self, CollectError> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| CollectError::Unavailable(e.to_string()))?;
            let docker = Docker::connect_with_local_defaults()
                .map_err(|e| CollectError::Unavailable(format!("docker: {e}")))?;
            rt.block_on(docker.ping())
                .map_err(|e| CollectError::Unavailable(format!("docker ping: {e}")))?;
            Ok(Self { docker, rt })
        }
    }

    impl DockerSource for LiveDocker {
        fn snapshot(&mut self) -> Result<DockerView, CollectError> {
            let docker = self.docker.clone();
            self.rt.block_on(async move { fetch(&docker).await })
        }
    }

    async fn fetch(docker: &Docker) -> Result<DockerView, CollectError> {
        let list_opts = ListContainersOptionsBuilder::default().all(true).build();
        let listed = docker
            .list_containers(Some(list_opts))
            .await
            .map_err(|e| CollectError::Unavailable(format!("docker containers: {e}")))?;
        let mut containers = Vec::new();
        for summary in listed.into_iter().take(80) {
            let summary = serde_json::to_value(&summary).unwrap_or(serde_json::Value::Null);
            let Some(name) = summary
                .pointer("/names/0")
                .and_then(|n| n.as_str())
                .map(|n| n.trim_start_matches('/').to_string())
                .filter(|n| !n.is_empty())
            else {
                continue;
            };
            let image = json_str(summary.get("image"));
            let mut state = json_str(summary.get("state"));
            let mut health = None;
            let mut restart_count = 0u64;
            let inspect_opts = InspectContainerOptions::default();
            if let Ok(info) = docker.inspect_container(&name, Some(inspect_opts)).await {
                let v = serde_json::to_value(&info).unwrap_or(serde_json::Value::Null);
                if let Some(s) = v.pointer("/state/status").and_then(|s| s.as_str()) {
                    state = s.to_string();
                }
                health = v
                    .pointer("/state/health/status")
                    .and_then(|s| s.as_str())
                    .map(str::to_string);
                restart_count = v
                    .get("restart_count")
                    .and_then(|n| n.as_u64())
                    .or_else(|| v.pointer("/restart_count").and_then(|n| n.as_u64()))
                    .unwrap_or(0);
            }
            let running = state.eq_ignore_ascii_case("running");
            let (cpu_pct, mem_bytes) = if running {
                one_stats(docker, &name).await
            } else {
                (None, None)
            };
            containers.push(ContainerSnap {
                name,
                image,
                state,
                health,
                restart_count,
                cpu_pct,
                mem_bytes,
            });
        }
        let image_opts = ListImagesOptionsBuilder::default().all(false).build();
        let images = docker
            .list_images(Some(image_opts))
            .await
            .map_err(|e| CollectError::Unavailable(format!("docker images: {e}")))?;
        let images = images
            .into_iter()
            .filter_map(|image| {
                let v = serde_json::to_value(&image).ok()?;
                let name = v
                    .get("repo_tags")
                    .and_then(|t| t.as_array())
                    .and_then(|tags| {
                        tags.iter().find_map(|t| {
                            t.as_str()
                                .filter(|s| !s.starts_with("<none>"))
                                .map(str::to_string)
                        })
                    })?;
                Some(ImageSnap {
                    name,
                    id: v
                        .get("id")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                    size_bytes: v.get("size").and_then(|n| n.as_u64()).unwrap_or(0),
                })
            })
            .take(40)
            .collect();
        Ok(DockerView { containers, images })
    }

    async fn one_stats(docker: &Docker, name: &str) -> (Option<f64>, Option<f64>) {
        let opts = StatsOptionsBuilder::default()
            .stream(false)
            .one_shot(true)
            .build();
        let mut stream = docker.stats(name, Some(opts));
        let Some(Ok(stat)) = stream.next().await else {
            return (None, None);
        };
        let v = serde_json::to_value(&stat).unwrap_or(serde_json::Value::Null);
        let cpu = num(v.pointer("/cpu_stats/cpu_usage/total_usage"));
        let prev = num(v.pointer("/precpu_stats/cpu_usage/total_usage"));
        let system = num(v.pointer("/cpu_stats/system_cpu_usage"));
        let system_prev = num(v.pointer("/precpu_stats/system_cpu_usage"));
        let online = num(v.pointer("/cpu_stats/online_cpus")).unwrap_or(1);
        let cpu_pct = match (cpu, prev, system, system_prev) {
            (Some(c), Some(p), Some(s), Some(sp)) => super::cpu_percent(c, p, s, sp, online),
            _ => None,
        };
        let mem = v
            .pointer("/memory_stats/usage")
            .and_then(|n| n.as_f64().or_else(|| n.as_u64().map(|u| u as f64)));
        (cpu_pct, mem)
    }

    fn json_str(v: Option<&serde_json::Value>) -> String {
        match v {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(other) => other.to_string().trim_matches('"').to_string(),
            None => String::new(),
        }
    }

    fn num(v: Option<&serde_json::Value>) -> Option<u64> {
        v.and_then(|n| n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)))
    }
}

#[cfg(feature = "docker")]
pub use live::LiveDocker;

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(Result<DockerView, String>);

    impl DockerSource for Fixed {
        fn snapshot(&mut self) -> Result<DockerView, CollectError> {
            self.0.clone().map_err(CollectError::Unavailable)
        }
    }

    #[test]
    fn containers_and_images_use_stable_ids() {
        let view = DockerView {
            containers: vec![ContainerSnap {
                name: "/api".into(),
                image: "library/api:1".into(),
                state: "running".into(),
                health: Some("healthy".into()),
                restart_count: 2,
                cpu_pct: Some(12.345),
                mem_bytes: Some(4096.0),
            }],
            images: vec![ImageSnap {
                name: "library/api:1".into(),
                id: "sha256:abc".into(),
                size_bytes: 100,
            }],
        };
        let obs = observe_docker("web1", &view);
        let ctr = obs
            .resources
            .iter()
            .find(|r| r.kind == ResourceKind::Container)
            .unwrap();
        assert_eq!(ctr.id, "ctr:web1:api");
        assert_eq!(ctr.parent.as_deref(), Some("host:web1"));
        assert_eq!(ctr.health, Health::Ok);
        assert_eq!(ctr.attrs["restart_count"], 2);
        let img = obs
            .resources
            .iter()
            .find(|r| r.kind == ResourceKind::Image)
            .unwrap();
        assert_eq!(img.id, "img:web1:library/api:1");
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "ctr.cpu_pct" && s.value == 12.35));
        assert!(obs.samples.iter().any(|s| s.metric == "ctr.mem_bytes"));
    }

    #[test]
    fn unhealthy_and_restarting_containers() {
        assert_eq!(
            container_health("running", Some("unhealthy"), 0),
            Health::Failed
        );
        assert_eq!(container_health("dead", None, 0), Health::Failed);
        assert_eq!(container_health("restarting", None, 0), Health::Degraded);
        assert_eq!(container_health("running", None, 8), Health::Degraded);
        assert_eq!(container_health("exited", None, 0), Health::Unknown);
    }

    #[test]
    fn missing_daemon_fails_the_pass() {
        let mut c = DockerCollector::new("web1", Fixed(Err("no such file".into())));
        let err = c.collect().unwrap_err();
        assert!(err.to_string().contains("no such file"));
    }

    #[test]
    fn cpu_percent_needs_a_delta() {
        assert!(cpu_percent(200, 100, 1000, 500, 2).unwrap() > 0.0);
        assert!(cpu_percent(100, 100, 1000, 1000, 1).is_none());
    }
}
