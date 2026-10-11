//! Top processes by CPU and memory.
//!
//! Only processes above a small floor are reported, and a process stays for a
//! few passes after it drops off, so the change journal is not a pid listing.

use std::collections::HashMap;
use std::ops::Deref;

use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};
use sysinfo::{ProcessesToUpdate, System, Users};

use crate::{CollectError, Collector};

/// Include a process once CPU or memory crosses this floor.
const CPU_KEEP: f64 = 5.0;
const MEM_KEEP: f64 = 3.0;
const MAX_PROCS: usize = 12;
/// Passes to keep reporting a process after it leaves the top set.
const STICKY_MISSES: u8 = 3;
const CPU_FAILED: f64 = 95.0;
const CPU_DEGRADED: f64 = 80.0;
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
        Ok(observation(&self.host, &choose(&procs, &mut self.sticky)))
    }
}

pub(crate) fn proc_health(cpu_pct: f64, mem_pct: f64) -> Health {
    if cpu_pct >= CPU_FAILED || mem_pct >= MEM_FAILED {
        Health::Failed
    } else if cpu_pct >= CPU_DEGRADED || mem_pct >= MEM_DEGRADED {
        Health::Degraded
    } else {
        Health::Ok
    }
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

fn observation(host: &str, procs: &[ProcView]) -> Observation {
    let parent = format!("host:{host}");
    let mut obs = Observation::default();
    for proc in procs {
        let id = format!("proc:{host}:{}", proc.pid);
        let mut resource = Resource::new(&id, ResourceKind::Process, &proc.name)
            .parent(&parent)
            .health(proc_health(proc.cpu_pct, proc.mem_pct))
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
        assert_eq!(proc_health(80.0, 1.0), Health::Degraded);
        assert_eq!(proc_health(10.0, 25.0), Health::Degraded);
        assert_eq!(proc_health(95.0, 1.0), Health::Failed);
        assert_eq!(proc_health(1.0, 50.0), Health::Failed);
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
    fn runaway_process_is_failed() {
        let obs = observation("h", &[proc(9, 99.0, 1.0)]);
        let resource = &obs.resources[0];
        assert_eq!(resource.id, "proc:h:9");
        assert_eq!(resource.kind, ResourceKind::Process);
        assert_eq!(resource.parent.as_deref(), Some("host:h"));
        assert_eq!(resource.health, Health::Failed);
        assert!(obs
            .samples
            .iter()
            .any(|s| s.metric == "proc.cpu_pct" && s.value == 99.0));
    }
}
