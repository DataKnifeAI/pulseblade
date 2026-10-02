//! Seed a fresh database with deterministic synthetic data for demos and screenshots.
//!
//! ```bash
//! cargo run -p pulseblade --example seed_demo -- /tmp/pb-demo/pb.db
//! pulseblade --db /tmp/pb-demo/pb.db node --no-collect --listen 127.0.0.1:7188
//! ```
//!
//! Replays two hours of 15-second collector passes from a fictional host
//! `demo-01`, ending now, through [`Store::apply`] and [`Store::record_run`], so
//! the change journal, samples, checkpoints, and collector runs are what a real
//! node would have recorded. In the last hour: config labels are applied, a
//! deploy restarts `app-web.service` and leaves `app-worker.service` crash
//! looping, then `backup.service` fills `/srv` past 90% and fails. Serve it
//! within a minute of seeding, before the collectors read as stale.

use std::f64::consts::TAU;
use std::path::PathBuf;

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use pulseblade_core::{
    CollectorRun, Health, LabelRule, Observation, Resource, ResourceKind, Sample,
};
use pulseblade_store::Store;

const HOST: &str = "demo-01";
const STEP_SECS: i64 = 15;
const STEPS: i64 = 2 * 3600 / STEP_SECS;
const GIB: u64 = 1024 * 1024 * 1024;
const MEM_TOTAL: u64 = 32 * GIB;
const CPUS: usize = 8;

/// Event times, in seconds before the last pass.
const LABELS_AT: i64 = 55 * 60;
const DEPLOY_AT: i64 = 25 * 60;
const WORKER_CRASH_AT: i64 = 23 * 60;
const BACKUP_START: i64 = 10 * 60;
const BACKUP_FAIL: i64 = 6 * 60;

/// (unit, description, unit file state, active, sub) in steady state.
#[rustfmt::skip]
const SERVICES: &[(&str, &str, &str, &str, &str)] = &[
    ("app-web.service", "Demo web application", "enabled", "active", "running"),
    ("app-worker.service", "Demo background worker", "enabled", "active", "running"),
    ("apt-daily.service", "Daily apt download activities", "static", "inactive", "dead"),
    ("backup.service", "Nightly backup to object storage", "static", "inactive", "dead"),
    ("chronyd.service", "NTP client/server", "enabled", "active", "running"),
    ("containerd.service", "containerd container runtime", "enabled", "active", "running"),
    ("cron.service", "Regular background program processing daemon", "enabled", "active", "running"),
    ("dbus.service", "D-Bus System Message Bus", "static", "active", "running"),
    ("docker.service", "Docker Application Container Engine", "enabled", "active", "running"),
    ("fail2ban.service", "Fail2Ban Service", "enabled", "active", "running"),
    ("getty@tty1.service", "Getty on tty1", "enabled", "active", "running"),
    ("logrotate.service", "Rotate log files", "static", "inactive", "dead"),
    ("nginx.service", "A high performance web server and a reverse proxy server", "enabled", "active", "running"),
    ("node-exporter.service", "Prometheus exporter for machine metrics", "enabled", "active", "running"),
    ("polkit.service", "Authorization Manager", "static", "active", "running"),
    ("postgresql.service", "PostgreSQL RDBMS", "enabled", "active", "exited"),
    ("postgresql@17-main.service", "PostgreSQL Cluster 17-main", "enabled-runtime", "active", "running"),
    ("redis-server.service", "Advanced key-value store", "enabled", "active", "running"),
    ("rsyslog.service", "System Logging Service", "enabled", "active", "running"),
    ("smartd.service", "Self Monitoring and Reporting Technology (SMART) Daemon", "enabled", "active", "running"),
    ("sshd.service", "OpenSSH server daemon", "enabled", "active", "running"),
    ("systemd-journald.service", "Journal Service", "static", "active", "running"),
    ("systemd-logind.service", "User Login Management", "static", "active", "running"),
    ("systemd-networkd.service", "Network Configuration", "enabled", "active", "running"),
    ("systemd-resolved.service", "Network Name Resolution", "enabled", "active", "running"),
    ("systemd-udevd.service", "Rule-based Manager for Device Events and Files", "static", "active", "running"),
    ("ufw.service", "Uncomplicated firewall", "enabled", "active", "exited"),
    ("unattended-upgrades.service", "Unattended Upgrades Shutdown", "enabled", "active", "running"),
    ("wg-quick@wg0.service", "WireGuard via wg-quick(8) for wg0", "enabled", "active", "exited"),
];

/// (mount, device, fs, kind, total bytes)
const DISKS: &[(&str, &str, &str, &str, u64)] = &[
    ("/", "/dev/nvme0n1p2", "ext4", "SSD", 100 * GIB),
    ("/var", "/dev/nvme0n1p3", "ext4", "SSD", 200 * GIB),
    ("/srv", "/dev/sda1", "xfs", "HDD", 2048 * GIB),
];

/// (iface, mac, addresses)
const IFACES: &[(&str, &str, &[&str])] = &[
    (
        "eth0",
        "02:00:00:00:00:01",
        &["192.0.2.10/24", "2001:db8::10/64"],
    ),
    (
        "wg0",
        "02:00:00:00:00:02",
        &["198.51.100.1/24", "2001:db8:100::1/64"],
    ),
];

fn main() -> anyhow::Result<()> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: seed_demo <db path>")?;
    anyhow::ensure!(
        !path.exists(),
        "{} already exists; seed into a fresh path",
        path.display()
    );
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let store = Store::open(&path).with_context(|| format!("opening {}", path.display()))?;
    seed(&store, Utc::now())?;
    let stats = store.stats()?;
    println!(
        "seeded {}: {} samples, {} changes, {} checkpoints",
        path.display(),
        stats.samples,
        stats.changes,
        stats.checkpoints
    );
    Ok(())
}

/// Write two hours of passes ending at `now`.
fn seed(store: &Store, now: DateTime<Utc>) -> anyhow::Result<()> {
    let labels = label_rules();
    let mut host = HostSim::new(now);
    for i in 0..=STEPS {
        let ago = (STEPS - i) * STEP_SECS;
        let ts = now - Duration::seconds(ago);
        if ago == DEPLOY_AT {
            store.create_checkpoint("before-deploy", ts - Duration::seconds(5))?;
        }
        let rules: &[LabelRule] = if ago <= LABELS_AT { &labels } else { &[] };
        let passes = [
            (
                "host",
                host.pass(i, ago),
                6 + (noise(i, 1).abs() * 8.0) as u64,
            ),
            (
                "systemd",
                services(ago),
                28 + (noise(i, 2).abs() * 17.0) as u64,
            ),
        ];
        for (source, mut obs, duration_ms) in passes {
            for r in &mut obs.resources {
                for rule in rules {
                    rule.apply(r);
                }
            }
            let changes = store.apply(source, ts, &obs)?;
            store.record_run(&CollectorRun {
                source: source.to_string(),
                ts,
                duration_ms,
                ok: true,
                error: None,
                resource_count: obs.resources.len(),
                sample_count: obs.samples.len(),
                change_count: changes.len(),
                interval_secs: Some(STEP_SECS as u64),
            })?;
        }
        if i == 0 {
            store.create_checkpoint("baseline", ts)?;
        }
    }
    Ok(())
}

fn label_rules() -> Vec<LabelRule> {
    let rule = |pattern: &str, set: &[(&str, &str)]| LabelRule {
        pattern: pattern.to_string(),
        set: set
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    vec![
        rule(
            "unit:*:postgresql*",
            &[("criticality", "high"), ("role", "database")],
        ),
        rule("disk:*:/srv", &[("role", "backups")]),
    ]
}

fn services(ago: i64) -> Observation {
    let parent = format!("host:{HOST}");
    let resources = SERVICES
        .iter()
        .map(|&(unit, description, file_state, active, sub)| {
            let (active, sub) = match unit {
                "app-web.service" if ago <= DEPLOY_AT && ago > DEPLOY_AT - 2 * STEP_SECS => {
                    ("activating", "start")
                }
                "app-worker.service" if ago <= WORKER_CRASH_AT => ("activating", "auto-restart"),
                "backup.service" if ago <= BACKUP_FAIL => ("failed", "failed"),
                "backup.service" if ago <= BACKUP_START => ("activating", "start"),
                _ => (active, sub),
            };
            Resource::new(format!("unit:{HOST}:{unit}"), ResourceKind::Service, unit)
                .parent(&parent)
                .health(match active {
                    "failed" => Health::Failed,
                    "activating" => Health::Degraded,
                    _ => Health::Ok,
                })
                .label("host", HOST)
                .attr("active", active)
                .attr("sub", sub)
                .attr("unit_file_state", file_state)
                .attr("description", description)
        })
        .collect();
    Observation {
        resources,
        samples: Vec::new(),
    }
}

/// Host, disk, and interface state with smoothed load averages.
struct HostSim {
    boot_time: String,
    load: [f64; 3],
}

impl HostSim {
    fn new(now: DateTime<Utc>) -> Self {
        let boot = now - Duration::days(12) - Duration::hours(4);
        Self {
            boot_time: DateTime::from_timestamp(boot.timestamp(), 0)
                .unwrap_or_default()
                .to_rfc3339(),
            load: [1.2, 1.2, 1.2],
        }
    }

    fn pass(&mut self, i: i64, ago: i64) -> Observation {
        let id = format!("host:{HOST}");
        let backup = ago <= BACKUP_START && ago > BACKUP_FAIL;
        let deploy = ago <= DEPLOY_AT && ago > DEPLOY_AT - 3 * 60;
        let wave = (TAU * i as f64 * STEP_SECS as f64 / 2400.0).sin();

        let cpu = if backup {
            84.0 + 6.0 * noise(i, 10)
        } else if deploy {
            46.0 + 8.0 * noise(i, 10)
        } else {
            15.0 + 5.0 * wave + 3.0 * noise(i, 10)
        };
        let busy = cpu / 100.0 * CPUS as f64 * 0.8;
        for (load, window) in self.load.iter_mut().zip([60.0, 300.0, 900.0]) {
            *load += (busy - *load) * (1.0 - (-(STEP_SECS as f64) / window).exp());
        }
        let mem = 38.0
            + 2.0 * (i as f64 / STEPS as f64)
            + if backup { 6.0 } else { 0.0 }
            + 0.4 * noise(i, 11);
        let available = MEM_TOTAL as f64 * (1.0 - mem / 100.0);

        let mut obs = Observation::default();
        obs.resources.push(
            Resource::new(&id, ResourceKind::Host, HOST)
                .health(Health::Ok)
                .label("host", HOST)
                .attr("os", "Linux (Debian 13)")
                .attr("kernel", "6.12.0")
                .attr("arch", "x86_64")
                .attr("cpu_count", CPUS)
                .attr("memory_total_bytes", MEM_TOTAL)
                .attr("swap_total_bytes", 4 * GIB)
                .attr("boot_time", self.boot_time.clone()),
        );
        obs.samples.extend([
            Sample::new(&id, "cpu.used_pct", cpu),
            Sample::new(&id, "load.1m", self.load[0]),
            Sample::new(&id, "load.5m", self.load[1]),
            Sample::new(&id, "load.15m", self.load[2]),
            Sample::new(&id, "mem.used_pct", mem),
            Sample::new(&id, "mem.available_bytes", available),
            Sample::new(&id, "swap.used_pct", 1.8 + 0.1 * noise(i, 12)),
        ]);

        let progress = i as f64 / STEPS as f64;
        for (n, &(mount, device, fs, kind, total)) in DISKS.iter().enumerate() {
            let used_pct = match mount {
                "/" => 46.8 + 0.3 * progress,
                "/var" => 63.0 + 1.5 * progress,
                _ => srv_used_pct(ago, progress),
            } + 0.02 * noise(i, 20 + n as u64);
            let disk_id = format!("disk:{HOST}:{mount}");
            obs.resources.push(
                Resource::new(&disk_id, ResourceKind::Disk, mount)
                    .parent(&id)
                    .health(if used_pct >= 90.0 {
                        Health::Degraded
                    } else {
                        Health::Ok
                    })
                    .label("host", HOST)
                    .attr("device", device)
                    .attr("fs", fs)
                    .attr("kind", kind)
                    .attr("read_only", false)
                    .attr("removable", false)
                    .attr("total_bytes", total),
            );
            obs.samples.extend([
                Sample::new(&disk_id, "disk.used_pct", used_pct),
                Sample::new(
                    &disk_id,
                    "disk.available_bytes",
                    total as f64 * (1.0 - used_pct / 100.0),
                ),
            ]);
        }

        for (n, &(iface, mac, addresses)) in IFACES.iter().enumerate() {
            let salt = 30 + 2 * n as u64;
            let (rx, tx) = match iface {
                "eth0" if backup => (1.6e6, 38.0e6),
                "eth0" if deploy => (6.5e6, 0.4e6),
                "eth0" => (1.4e6 * (1.0 + 0.3 * wave), 0.35e6),
                _ => (40e3, 25e3),
            };
            let net_id = format!("net:{HOST}:{iface}");
            obs.resources.push(
                Resource::new(&net_id, ResourceKind::NetIface, iface)
                    .parent(&id)
                    .health(Health::Ok)
                    .label("host", HOST)
                    .attr("mac", mac)
                    .attr("addresses", addresses.to_vec()),
            );
            obs.samples.extend([
                Sample::new(
                    &net_id,
                    "net.rx_bytes_per_s",
                    rx * (1.0 + 0.15 * noise(i, salt)),
                ),
                Sample::new(
                    &net_id,
                    "net.tx_bytes_per_s",
                    tx * (1.0 + 0.15 * noise(i, salt + 1)),
                ),
                Sample::new(&net_id, "net.rx_errors", 0.0),
                Sample::new(&net_id, "net.tx_errors", 0.0),
            ]);
        }
        obs
    }
}

/// Slow growth, then the backup writes to `/srv` and pushes it past 90%.
fn srv_used_pct(ago: i64, progress: f64) -> f64 {
    let steady = 86.0 + 3.4 * progress;
    if ago > BACKUP_START {
        steady
    } else {
        let written = (BACKUP_START - ago.max(BACKUP_FAIL)) as f64 / 60.0;
        steady + 0.45 * written
    }
}

/// Deterministic noise in `[-1, 1)` (splitmix64 of the step and a salt).
fn noise(i: i64, salt: u64) -> f64 {
    let mut z = (i as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(salt.wrapping_mul(0xD1B5_4A32_D192_ED03));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulseblade_core::Since;
    use pulseblade_mcp::query;
    use pulseblade_store::{ChangeFilter, ResourceFilter};

    fn of_kind(store: &Store, kind: ResourceKind) -> Vec<Resource> {
        store
            .resources(&ResourceFilter {
                kind: Some(kind),
                ..Default::default()
            })
            .unwrap()
            .into_iter()
            .map(|r| r.resource)
            .collect()
    }

    fn with_health(rs: &[Resource], health: Health) -> Vec<&str> {
        rs.iter()
            .filter(|r| r.health == health)
            .map(|r| r.name.as_str())
            .collect()
    }

    #[test]
    fn seeds_expected_shape() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();
        seed(&store, now).unwrap();

        let hosts = of_kind(&store, ResourceKind::Host);
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0].id, "host:demo-01");

        let services = of_kind(&store, ResourceKind::Service);
        assert_eq!(services.len(), SERVICES.len());
        assert_eq!(with_health(&services, Health::Failed), ["backup.service"]);
        assert_eq!(
            with_health(&services, Health::Degraded),
            ["app-worker.service"]
        );
        let disks = of_kind(&store, ResourceKind::Disk);
        assert_eq!(with_health(&disks, Health::Degraded), ["/srv"]);
        assert_eq!(of_kind(&store, ResourceKind::NetIface).len(), 2);

        let last_hour = store
            .resolve_since(&Since::Time(now - Duration::hours(1)))
            .unwrap();
        let (changes, _) = store
            .changes_after(last_hour, &ChangeFilter::default(), 1000)
            .unwrap();
        for field in ["health", "labels.role", "attrs.sub"] {
            assert!(
                changes.iter().any(|c| c.field.as_deref() == Some(field)),
                "no `{field}` change in the last hour"
            );
        }

        let stats = store.stats().unwrap();
        assert!(stats.samples > 0);
        assert_eq!(stats.checkpoints, 2);
        let cpu = store.latest("host:demo-01").unwrap()["cpu.used_pct"];
        assert_eq!(cpu.ts.timestamp_millis(), now.timestamp_millis());

        let collectors = query::collectors(&store).unwrap();
        assert_eq!(collectors.len(), 2);
        assert!(collectors.iter().all(|c| c.ok && !c.stale));
    }

    #[test]
    fn seeding_is_deterministic() {
        let now = Utc::now();
        let [a, b] = [(); 2].map(|_| {
            let store = Store::open_in_memory().unwrap();
            seed(&store, now).unwrap();
            store.latest_all().unwrap()
        });
        assert_eq!(a, b);
    }
}
