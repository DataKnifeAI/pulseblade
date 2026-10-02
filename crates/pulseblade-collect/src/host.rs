use std::collections::BTreeSet;
use std::time::Instant;

use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};
use sysinfo::{Disks, Networks, System};

use crate::{CollectError, Collector};

/// Disk usage at or above this percentage marks the disk degraded.
const DISK_DEGRADED_PCT: f64 = 90.0;
/// Disk usage at or above this percentage marks the disk failed.
const DISK_FAILED_PCT: f64 = 98.0;
/// Available memory below this percentage marks the host degraded.
const MEM_DEGRADED_AVAILABLE_PCT: f64 = 5.0;

/// Host, CPU, memory, disk, and network interface state via `sysinfo`.
pub struct HostCollector {
    host: String,
    sys: System,
    disks: Disks,
    networks: Networks,
    last_net_refresh: Instant,
}

impl HostCollector {
    pub fn new(host: String) -> Self {
        let mut sys = System::new();
        sys.refresh_cpu_usage();
        Self {
            host,
            sys,
            disks: Disks::new_with_refreshed_list(),
            networks: Networks::new_with_refreshed_list(),
            last_net_refresh: Instant::now(),
        }
    }

    fn host_id(&self) -> String {
        format!("host:{}", self.host)
    }

    fn collect_host(&mut self, obs: &mut Observation) {
        self.sys.refresh_cpu_usage();
        self.sys.refresh_memory();
        let id = self.host_id();

        let total = self.sys.total_memory() as f64;
        let available = self.sys.available_memory() as f64;
        let available_pct = pct(available, total);
        let health = if total > 0.0 && available_pct < MEM_DEGRADED_AVAILABLE_PCT {
            Health::Degraded
        } else {
            Health::Ok
        };

        let boot = chrono::DateTime::from_timestamp(System::boot_time() as i64, 0)
            .map(|t| t.to_rfc3339())
            .unwrap_or_default();
        obs.resources.push(
            Resource::new(&id, ResourceKind::Host, &self.host)
                .health(health)
                .label("host", &self.host)
                .attr("os", System::long_os_version().unwrap_or_default())
                .attr("kernel", System::kernel_version().unwrap_or_default())
                .attr("arch", System::cpu_arch())
                .attr("cpu_count", self.sys.cpus().len())
                .attr("memory_total_bytes", self.sys.total_memory())
                .attr("swap_total_bytes", self.sys.total_swap())
                .attr("boot_time", boot),
        );

        let load = System::load_average();
        let swap_total = self.sys.total_swap() as f64;
        obs.samples.extend([
            Sample::new(&id, "cpu.used_pct", self.sys.global_cpu_usage() as f64),
            Sample::new(&id, "load.1m", load.one),
            Sample::new(&id, "load.5m", load.five),
            Sample::new(&id, "load.15m", load.fifteen),
            Sample::new(&id, "mem.used_pct", 100.0 - available_pct),
            Sample::new(&id, "mem.available_bytes", available),
        ]);
        if swap_total > 0.0 {
            obs.samples.push(Sample::new(
                &id,
                "swap.used_pct",
                pct(self.sys.used_swap() as f64, swap_total),
            ));
        }
    }

    fn collect_disks(&mut self, obs: &mut Observation) {
        self.disks.refresh(true);
        let parent = self.host_id();
        let mut seen = BTreeSet::new();
        for disk in self.disks.list() {
            let mount = disk.mount_point().to_string_lossy().to_string();
            let total = disk.total_space() as f64;
            if total == 0.0 || !seen.insert(mount.clone()) {
                continue;
            }
            let used_pct = pct(total - disk.available_space() as f64, total);
            let health = disk_health(used_pct);
            let id = format!("disk:{}:{}", self.host, mount);
            obs.resources.push(
                Resource::new(&id, ResourceKind::Disk, &mount)
                    .parent(&parent)
                    .health(health)
                    .label("host", &self.host)
                    .attr("device", disk.name().to_string_lossy().to_string())
                    .attr("fs", disk.file_system().to_string_lossy().to_string())
                    .attr("kind", disk.kind().to_string())
                    .attr("read_only", disk.is_read_only())
                    .attr("removable", disk.is_removable())
                    .attr("total_bytes", disk.total_space()),
            );
            obs.samples.extend([
                Sample::new(&id, "disk.used_pct", used_pct),
                Sample::new(&id, "disk.available_bytes", disk.available_space() as f64),
            ]);
        }
    }

    fn collect_networks(&mut self, obs: &mut Observation) {
        self.networks.refresh(true);
        let elapsed = self.last_net_refresh.elapsed().as_secs_f64().max(0.001);
        self.last_net_refresh = Instant::now();
        let parent = self.host_id();
        for (name, data) in self.networks.list() {
            if skip_iface(name) {
                continue;
            }
            let id = format!("net:{}:{}", self.host, name);
            let mut ips: Vec<String> = data
                .ip_networks()
                .iter()
                .map(|n| format!("{}/{}", n.addr, n.prefix))
                .collect();
            ips.sort();
            let health = if ips.is_empty() {
                Health::Unknown
            } else {
                Health::Ok
            };
            obs.resources.push(
                Resource::new(&id, ResourceKind::NetIface, name)
                    .parent(&parent)
                    .health(health)
                    .label("host", &self.host)
                    .attr("mac", data.mac_address().to_string())
                    .attr("addresses", ips),
            );
            obs.samples.extend([
                Sample::new(&id, "net.rx_bytes_per_s", data.received() as f64 / elapsed),
                Sample::new(
                    &id,
                    "net.tx_bytes_per_s",
                    data.transmitted() as f64 / elapsed,
                ),
                Sample::new(&id, "net.rx_errors", data.errors_on_received() as f64),
                Sample::new(&id, "net.tx_errors", data.errors_on_transmitted() as f64),
            ]);
        }
    }
}

impl Collector for HostCollector {
    fn name(&self) -> &'static str {
        "host"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        let mut obs = Observation::default();
        self.collect_host(&mut obs);
        self.collect_disks(&mut obs);
        self.collect_networks(&mut obs);
        Ok(obs)
    }
}

fn pct(part: f64, total: f64) -> f64 {
    if total > 0.0 {
        part / total * 100.0
    } else {
        0.0
    }
}

fn disk_health(used_pct: f64) -> Health {
    if used_pct >= DISK_FAILED_PCT {
        Health::Failed
    } else if used_pct >= DISK_DEGRADED_PCT {
        Health::Degraded
    } else {
        Health::Ok
    }
}

/// Loopback and per-container veth pairs churn constantly and carry no host signal.
fn skip_iface(name: &str) -> bool {
    name == "lo" || name.starts_with("veth")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_thresholds() {
        assert_eq!(disk_health(50.0), Health::Ok);
        assert_eq!(disk_health(90.0), Health::Degraded);
        assert_eq!(disk_health(99.0), Health::Failed);
    }

    #[test]
    fn collects_this_host() {
        let mut c = HostCollector::new("test".into());
        let obs = c.collect().unwrap();
        let host = obs
            .resources
            .iter()
            .find(|r| r.kind == ResourceKind::Host)
            .unwrap();
        assert_eq!(host.id, "host:test");
        assert!(obs.samples.iter().any(|s| s.metric == "cpu.used_pct"));
        assert!(obs
            .resources
            .iter()
            .filter(|r| r.kind != ResourceKind::Host)
            .all(|r| r.parent.as_deref() == Some("host:test")));
    }
}
