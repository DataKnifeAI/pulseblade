//! Top processes by CPU and memory.
//!
//! Only processes above a small floor are reported, and a process stays for a
//! few passes after it drops off, so the change journal is not a pid listing.
//! `proc.cpu_pct` is the one-core sample (100 = one core). Stored health uses
//! that divided by the host CPU count, so 100 is the whole machine.

use std::collections::HashMap;
use std::ops::Deref;

use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};
use sysinfo::{CpuRefreshKind, ProcessesToUpdate, System, Users};

use crate::{CollectError, Collector};

/// Include a process once CPU or memory crosses this floor.
const CPU_KEEP: f64 = 5.0;
const MEM_KEEP: f64 = 3.0;
const MAX_PROCS: usize = 12;
/// Passes to keep reporting a process after it leaves the top set.
const STICKY_MISSES: u8 = 3;
/// Host CPU share at or above this is failed. 100 is the whole machine.
const CPU_SHARE_FAILED: f64 = 90.0;
const MEM_FAILED: f64 = 50.0;
const MEM_DEGRADED: f64 = 25.0;

pub struct ProcessCollector {
    host: String,
    sys: System,
    /// Pid to consecutive passes spent below the keep floor.
    sticky: HashMap<u32, u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct ProcView {
    pub pid: u32,
    pub name: String,
    pub cmd: String,
    pub user: Option<String>,
    pub cpu_pct: f64,
    pub mem_pct: f64,
    pub rss_bytes: u64,
}

impl ProcessCollector {
    pub fn new(host: String) -> Self {
        Self {
            host,
            sys: System::new(),
            sticky: HashMap::new(),
        }
    }
}

impl Collector for ProcessCollector {
    fn name(&self) -> &'static str {
        "processes"
    }

    fn collect(&mut self) -> Result<Observation, CollectError> {
        self.sys.refresh_memory();
        self.sys.refresh_processes(ProcessesToUpdate::All, true);
        let total = self.sys.total_memory().max(1) as f64;
        let users = Users::new_with_refreshed_list();
        let procs: Vec<ProcView> = self
            .sys
            .processes()
            .iter()
            .filter_map(|(pid, proc)| {
                let pid = pid.as_u32();
                if pid == 0 {
                    return None;
                }
                let name = proc.name().to_string_lossy().trim().to_string();
                if name.is_empty() {
                    return None;
                }
                let rss = proc.memory();
                let mem_pct = rss as f64 / total * 100.0;
                let user = proc.user_id().map(|uid| {
                    users
                        .get_user_by_id(uid)
                        .map(|u| u.name().to_string())
                        .unwrap_or_else(|| uid.deref().to_string())
                });
                Some(ProcView {
                    pid,
                    name,
                    cmd: clip_cmd(proc.cmd()),
                    user,
                    cpu_pct: f64::from(proc.cpu_usage()),
                    mem_pct,
                    rss_bytes: rss,
                })
            })
            .collect();
        if self.sys.cpus().is_empty() {
            self.sys.refresh_cpu_list(CpuRefreshKind::nothing());
        }
        let cpu_count = self.sys.cpus().len();
        Ok(observation(
            &self.host,
            cpu_count,
            &choose(&procs, &mut self.sticky),
        ))
    }
}

/// Stored health from host CPU share and memory percent.
///
/// CPU changes health only at [`CPU_SHARE_FAILED`]: 50% of the host up to that
/// line is busy on the dashboard and stays [`Health::Ok`]. Memory is degraded
/// at 25% and failed at 50%.
pub(crate) fn proc_health(cpu_share: f64, mem_pct: f64) -> Health {
    if cpu_share >= CPU_SHARE_FAILED || mem_pct >= MEM_FAILED {
        Health::Failed
    } else if mem_pct >= MEM_DEGRADED {
        Health::Degraded
    } else {
        Health::Ok
    }
}

fn cpu_share(cpu_pct: f64, cpu_count: usize) -> f64 {
    cpu_pct / cpu_count.max(1) as f64
}

/// Processes worth tracking this pass, including a short sticky tail.
pub(crate) fn choose(procs: &[ProcView], sticky: &mut HashMap<u32, u8>) -> Vec<ProcView> {
    let mut ranked: Vec<ProcView> = procs
        .iter()
        .filter(|p| p.cpu_pct >= CPU_KEEP || p.mem_pct >= MEM_KEEP)
        .cloned()
        .collect();
    ranked.sort_by(|a, b| {
        b.cpu_pct
            .partial_cmp(&a.cpu_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                b.mem_pct
                    .partial_cmp(&a.mem_pct)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    ranked.truncate(MAX_PROCS);

    let mut chosen: HashMap<u32, ProcView> = ranked.into_iter().map(|p| (p.pid, p)).collect();
    let mut next = HashMap::new();
    for pid in chosen.keys() {
        next.insert(*pid, 0);
    }
    for (pid, misses) in sticky.iter() {
        if chosen.contains_key(pid) {
            continue;
        }
        let Some(proc) = procs.iter().find(|p| p.pid == *pid) else {
            continue;
        };
        let misses = misses.saturating_add(1);
        if misses <= STICKY_MISSES {
            chosen.insert(*pid, proc.clone());
            next.insert(*pid, misses);
        }
    }
    *sticky = next;
    let mut out: Vec<_> = chosen.into_values().collect();
    out.sort_by_key(|p| p.pid);
    out
}

fn observation(host: &str, cpu_count: usize, procs: &[ProcView]) -> Observation {
    let parent = format!("host:{host}");
    let mut obs = Observation::default();
    for proc in procs {
        let id = format!("proc:{host}:{}", proc.pid);
        let share = cpu_share(proc.cpu_pct, cpu_count);
        let mut resource = Resource::new(&id, ResourceKind::Process, &proc.name)
            .parent(&parent)
            .health(proc_health(share, proc.mem_pct))
            .label("host", host)
            .attr("pid", proc.pid);
        if let Some(user) = &proc.user {
            resource = resource.attr("user", user.clone());
        }
        if !proc.cmd.is_empty() {
            resource = resource.attr("cmd", proc.cmd.clone());
        }
        obs.resources.push(resource);
        obs.samples.extend([
            Sample::new(&id, "proc.cpu_pct", proc.cpu_pct),
            Sample::new(&id, "proc.host_cpu_pct", share),
            Sample::new(&id, "proc.mem_pct", proc.mem_pct),
            Sample::new(&id, "proc.rss_bytes", proc.rss_bytes as f64),
        ]);
    }
    obs
}

fn clip_cmd(cmd: &[std::ffi::OsString]) -> String {
    let joined = cmd
        .iter()
        .map(|c| c.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");
    let flat = joined.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= 120 {
        flat
    } else {
        let mut out: String = flat.chars().take(119).collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proc(pid: u32, cpu: f64, mem: f64) -> ProcView {
        ProcView {
            pid,
            name: format!("p{pid}"),
            cmd: String::new(),
            user: None,
            cpu_pct: cpu,
            mem_pct: mem,
            rss_bytes: 0,
        }
    }

    #[test]
    fn health_thresholds() {
        assert_eq!(proc_health(10.0, 1.0), Health::Ok);
        // ~10 of 32 cores, with modest memory, stays ok.
        assert_eq!(proc_health(1000.0 / 32.0, 12.0), Health::Ok);
        // 50% up to 90% of the host is busy on the dashboard only.
        assert_eq!(proc_health(50.0, 1.0), Health::Ok);
        assert_eq!(proc_health(89.9, 1.0), Health::Ok);
        assert_eq!(proc_health(10.0, 24.9), Health::Ok);
        assert_eq!(proc_health(10.0, 25.0), Health::Degraded);
        assert_eq!(proc_health(90.0, 1.0), Health::Failed);
        assert_eq!(proc_health(1.0, 50.0), Health::Failed);
        assert_eq!(proc_health(90.0, 12.0), Health::Failed);
    }

    #[test]
    fn keeps_hot_processes_and_sticks_briefly() {
        let procs = vec![proc(1, 40.0, 1.0), proc(2, 1.0, 1.0), proc(3, 1.0, 10.0)];
        let mut sticky = HashMap::new();
        let chosen = choose(&procs, &mut sticky);
        let ids: Vec<_> = chosen.iter().map(|p| p.pid).collect();
        assert_eq!(ids, vec![1, 3]);

        let quiet = vec![proc(1, 0.1, 0.1), proc(2, 0.1, 0.1)];
        let still = choose(&quiet, &mut sticky);
        assert_eq!(still.len(), 1);
        assert_eq!(still[0].pid, 1);

        for _ in 0..STICKY_MISSES {
            choose(&quiet, &mut sticky);
        }
        assert!(choose(&quiet, &mut sticky).is_empty());
    }

    #[test]
    fn wide_host_keeps_a_few_busy_cores_ok() {
        let obs = observation("h", 32, &[proc(9, 1000.0, 12.0)]);
        let resource = &obs.resources[0];
        assert_eq!(resource.health, Health::Ok);
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "proc.cpu_pct" && s.value == 1000.0));
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "proc.host_cpu_pct" && s.value == 1000.0 / 32.0));
        // The old one-core 95% line is not a host-share failure.
        assert_eq!(
            observation("h", 32, &[proc(9, 95.0, 1.0)]).resources[0].health,
            Health::Ok
        );
        // Half the machine is still stored as ok.
        assert_eq!(
            observation("h", 32, &[proc(9, 1600.0, 1.0)]).resources[0].health,
            Health::Ok
        );
    }

    #[test]
    fn host_share_at_least_90_is_failed() {
        let obs = observation("h", 32, &[proc(9, 32.0 * 90.0, 1.0)]);
        let resource = &obs.resources[0];
        assert_eq!(resource.id, "proc:h:9");
        assert_eq!(resource.kind, ResourceKind::Process);
        assert_eq!(resource.parent.as_deref(), Some("host:h"));
        assert_eq!(resource.health, Health::Failed);
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "proc.cpu_pct" && s.value == 2880.0));
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "proc.host_cpu_pct" && s.value == 90.0));
    }
}
