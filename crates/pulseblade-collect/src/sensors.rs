//! Host sensors that can change a health call.
//!
//! Temperatures and fans use the chip's own limit or alarm. A reading with
//! no limit is recorded and left ok. Memory and I/O stall, uncorrectable
//! ECC, a rising thermal-throttle count, and an NVMe controller that is not
//! live are failures on their own. Unused fan headers (0 RPM, no minimum, no
//! alarm) are skipped.

use std::fs;
use std::path::{Path, PathBuf};

use pulseblade_core::{Health, Observation, Resource, ResourceKind, Sample};

const PRESSURE_DEGRADED: f64 = 20.0;
const PRESSURE_FAILED: f64 = 50.0;
const TEMP_ABSENT_C: f64 = 0.0;
const TEMP_LOW_C: f64 = -40.0;
const TEMP_HIGH_C: f64 = 200.0;

pub(crate) fn collect(host: &str, throttle_prev: &mut Option<u64>) -> Observation {
    collect_at(host, &sysfs_root(), &proc_root(), throttle_prev)
}

pub(crate) fn collect_at(
    host: &str,
    sysfs: &Path,
    procfs: &Path,
    throttle_prev: &mut Option<u64>,
) -> Observation {
    let mut obs = Observation::default();
    read_hwmon(host, sysfs, &mut obs);
    read_pressure(host, procfs, &mut obs);
    read_edac(host, sysfs, &mut obs);
    read_throttle(host, sysfs, throttle_prev, &mut obs);
    read_nvme(host, sysfs, &mut obs);
    obs
}

fn sysfs_root() -> PathBuf {
    if let Some(path) = std::env::var_os("PULSEBLADE_SYSFS") {
        return PathBuf::from(path);
    }
    let mounted = PathBuf::from("/host/sys");
    if mounted.join("class/hwmon").is_dir() {
        mounted
    } else {
        PathBuf::from("/sys")
    }
}

fn proc_root() -> PathBuf {
    if let Some(path) = std::env::var_os("PULSEBLADE_PROC") {
        return PathBuf::from(path);
    }
    if Path::new("/host/proc/pressure").is_dir() {
        PathBuf::from("/host/proc")
    } else {
        PathBuf::from("/proc")
    }
}

fn read_hwmon(host: &str, sysfs: &Path, obs: &mut Observation) {
    let root = sysfs.join("class/hwmon");
    let mut dirs = match fs::read_dir(&root) {
        Ok(dirs) => dirs.filter_map(|e| e.ok()).collect::<Vec<_>>(),
        Err(_) => return,
    };
    dirs.sort_by_key(|e| e.file_name());
    for entry in dirs {
        let dir = entry.path();
        let raw_name = read_text(&dir.join("name")).unwrap_or_default();
        let chip = chip_key(&dir, &raw_name);
        read_temps(host, &dir, &chip, &raw_name, obs);
        read_fans(host, &dir, &chip, obs);
        read_power(host, &dir, &chip, obs);
    }
}

fn chip_key(dir: &Path, raw_name: &str) -> String {
    if raw_name == "nvme" {
        if let Some(dev) = device_name(dir) {
            return slug(&dev);
        }
    }
    let name = if raw_name.is_empty() {
        dir.file_name().and_then(|n| n.to_str()).unwrap_or("hwmon")
    } else {
        raw_name
    };
    let key = slug(name);
    if key.is_empty() {
        "hwmon".into()
    } else {
        key
    }
}

fn device_name(dir: &Path) -> Option<String> {
    let target = fs::read_link(dir.join("device")).ok()?;
    target
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_string)
}

fn read_temps(host: &str, dir: &Path, chip: &str, raw_name: &str, obs: &mut Observation) {
    for (index, path) in channels(dir, "temp") {
        let Some(milli) = read_number(&path) else {
            continue;
        };
        let celsius = milli / 1000.0;
        if celsius == TEMP_ABSENT_C || !(TEMP_LOW_C..=TEMP_HIGH_C).contains(&celsius) {
            continue;
        }
        let label = read_text(&dir.join(format!("temp{index}_label"))).unwrap_or_default();
        let alarm = read_number(&dir.join(format!("temp{index}_alarm"))).unwrap_or(0.0) >= 1.0;
        let crit = read_number(&dir.join(format!("temp{index}_crit"))).map(|v| v / 1000.0);
        let max = read_number(&dir.join(format!("temp{index}_max"))).map(|v| v / 1000.0);
        if raw_name == "acpitz" && !alarm && crit.is_none() && max.is_none() {
            continue;
        }
        let (health, fault) = if alarm || crit.is_some_and(|limit| celsius >= limit) {
            (Health::Failed, Some("thermal critical"))
        } else if max.is_some_and(|limit| celsius >= limit) {
            (Health::Degraded, Some("thermal warning"))
        } else {
            (Health::Ok, None)
        };
        let channel = if label.is_empty() {
            format!("temp{index}")
        } else {
            label.clone()
        };
        push(
            obs,
            host,
            "temp",
            chip,
            &channel,
            &label_or(&label, &channel),
            "temp",
            health,
            fault,
            Some(("temp.c", round2(celsius))),
        );
    }
}

fn read_fans(host: &str, dir: &Path, chip: &str, obs: &mut Observation) {
    for (index, path) in channels(dir, "fan") {
        let Some(rpm) = read_number(&path) else {
            continue;
        };
        let min = read_number(&dir.join(format!("fan{index}_min"))).filter(|v| *v > 0.0);
        let alarm = read_number(&dir.join(format!("fan{index}_alarm"))).unwrap_or(0.0) >= 1.0;
        if rpm <= 0.0 && min.is_none() && !alarm {
            continue;
        }
        let label = read_text(&dir.join(format!("fan{index}_label"))).unwrap_or_default();
        let (health, fault) = if alarm || rpm <= 0.0 || min.is_some_and(|floor| rpm < floor) {
            (Health::Failed, Some("fan stopped"))
        } else {
            (Health::Ok, None)
        };
        let channel = if label.is_empty() {
            format!("fan{index}")
        } else {
            label.clone()
        };
        push(
            obs,
            host,
            "fan",
            chip,
            &channel,
            &label_or(&label, &channel),
            "fan",
            health,
            fault,
            Some(("fan.rpm", rpm)),
        );
    }
}

fn read_power(host: &str, dir: &Path, chip: &str, obs: &mut Observation) {
    for (index, path) in channels(dir, "power") {
        let Some(microwatts) = read_number(&path) else {
            continue;
        };
        if microwatts <= 0.0 {
            continue;
        }
        let watts = microwatts / 1_000_000.0;
        let alarm = read_number(&dir.join(format!("power{index}_alarm"))).unwrap_or(0.0) >= 1.0;
        let crit = read_number(&dir.join(format!("power{index}_crit"))).map(|v| v / 1_000_000.0);
        let (health, fault) = if alarm || crit.is_some_and(|limit| watts >= limit) {
            (Health::Failed, Some("power critical"))
        } else {
            (Health::Ok, None)
        };
        let label = read_text(&dir.join(format!("power{index}_label"))).unwrap_or_default();
        let channel = if label.is_empty() {
            format!("power{index}")
        } else {
            label.clone()
        };
        push(
            obs,
            host,
            "sensor",
            chip,
            &channel,
            &label_or(&label, &channel),
            "power",
            health,
            fault,
            Some(("power.watts", round2(watts))),
        );
    }
}

fn read_pressure(host: &str, procfs: &Path, obs: &mut Observation) {
    let cpu = psi_some_avg10(&procfs.join("pressure/cpu"));
    let memory = psi_some_avg10(&procfs.join("pressure/memory"));
    let io = psi_some_avg10(&procfs.join("pressure/io"));
    if cpu.is_none() && memory.is_none() && io.is_none() {
        return;
    }
    let mut health = Health::Ok;
    let mut faults = Vec::new();
    for (value, fault) in [(memory, "memory pressure"), (io, "io pressure")] {
        let Some(value) = value else {
            continue;
        };
        if value >= PRESSURE_FAILED {
            health = Health::Failed;
            faults.push(fault);
        } else if value >= PRESSURE_DEGRADED {
            health = worse(health, Health::Degraded);
            faults.push(fault);
        }
    }
    let fault_text = faults.join("; ");
    let fault = if fault_text.is_empty() {
        None
    } else {
        Some(fault_text.as_str())
    };
    let id = push(
        obs, host, "sensor", "pressure", "", "pressure", "pressure", health, fault, None,
    );
    for (metric, value) in [
        ("pressure.cpu_pct", cpu),
        ("pressure.memory_pct", memory),
        ("pressure.io_pct", io),
    ] {
        if let Some(value) = value {
            obs.samples.push(Sample::new(&id, metric, round2(value)));
        }
    }
}

fn read_edac(host: &str, sysfs: &Path, obs: &mut Observation) {
    let root = sysfs.join("devices/system/edac/mc");
    let mut dirs = match fs::read_dir(&root) {
        Ok(dirs) => dirs.filter_map(|e| e.ok()).collect::<Vec<_>>(),
        Err(_) => return,
    };
    dirs.retain(|e| {
        e.file_name().to_str().is_some_and(|name| {
            name.starts_with("mc") && name[2..].chars().all(|c| c.is_ascii_digit())
        })
    });
    if dirs.is_empty() {
        return;
    }
    let mut corrected = 0.0;
    let mut uncorrectable = 0.0;
    for entry in dirs {
        corrected += read_number(&entry.path().join("ce_count")).unwrap_or(0.0);
        uncorrectable += read_number(&entry.path().join("ue_count")).unwrap_or(0.0);
    }
    let (health, fault) = if uncorrectable > 0.0 {
        (Health::Failed, Some("uncorrectable memory errors"))
    } else if corrected > 0.0 {
        (Health::Degraded, Some("corrected memory errors"))
    } else {
        (Health::Ok, None)
    };
    let id = push(
        obs, host, "sensor", "edac", "", "edac", "edac", health, fault, None,
    );
    obs.samples.push(Sample::new(&id, "edac.ce", corrected));
    obs.samples.push(Sample::new(&id, "edac.ue", uncorrectable));
}

fn read_throttle(host: &str, sysfs: &Path, prev: &mut Option<u64>, obs: &mut Observation) {
    let total = throttle_total(sysfs);
    let Some(total) = total else {
        return;
    };
    let delta = match *prev {
        Some(before) => total.saturating_sub(before),
        None => 0,
    };
    *prev = Some(total);
    let (health, fault) = if delta > 0 {
        (Health::Degraded, Some("cpu throttled"))
    } else {
        (Health::Ok, None)
    };
    let id = push(
        obs,
        host,
        "sensor",
        "throttle",
        "",
        "throttle",
        "throttle",
        health,
        fault,
        Some(("cpu.throttle", delta as f64)),
    );
    let _ = id;
}

fn throttle_total(sysfs: &Path) -> Option<u64> {
    let root = sysfs.join("devices/system/cpu");
    let dirs = fs::read_dir(&root).ok()?;
    let mut total = 0_u64;
    let mut found = false;
    for entry in dirs.filter_map(|e| e.ok()) {
        let name = entry.file_name();
        let name = name.to_str().unwrap_or("");
        if !(name.starts_with("cpu") && name[3..].chars().all(|c| c.is_ascii_digit())) {
            continue;
        }
        let dir = entry.path().join("thermal_throttle");
        let count = read_number(&dir.join("package_throttle_count"))
            .or_else(|| read_number(&dir.join("core_throttle_count")));
        if let Some(count) = count {
            found = true;
            total = total.saturating_add(count as u64);
        }
    }
    found.then_some(total)
}

fn read_nvme(host: &str, sysfs: &Path, obs: &mut Observation) {
    let root = sysfs.join("class/nvme");
    let mut dirs = match fs::read_dir(&root) {
        Ok(dirs) => dirs.filter_map(|e| e.ok()).collect::<Vec<_>>(),
        Err(_) => return,
    };
    dirs.sort_by_key(|e| e.file_name());
    for entry in dirs {
        let Some(state) = read_text(&entry.path().join("state")) else {
            continue;
        };
        if state.is_empty() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let health = if state.eq_ignore_ascii_case("live") {
            Health::Ok
        } else {
            Health::Failed
        };
        let fault = if health == Health::Failed {
            Some("controller not live")
        } else {
            None
        };
        let id = push(
            obs,
            host,
            "sensor",
            &slug(&name),
            "state",
            &name,
            "nvme",
            health,
            fault,
            None,
        );
        if let Some(resource) = obs.resources.iter_mut().find(|r| r.id == id) {
            *resource = resource.clone().attr("state", state);
        }
    }
}

fn channels(dir: &Path, prefix: &str) -> Vec<(u32, PathBuf)> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.filter_map(|e| e.ok()) {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(index) = channel_index(name, prefix) else {
            continue;
        };
        out.push((index, entry.path()));
    }
    out.sort_by_key(|(index, _)| *index);
    out
}

fn channel_index(name: &str, prefix: &str) -> Option<u32> {
    let rest = name.strip_prefix(prefix)?.strip_suffix("_input")?;
    rest.parse().ok()
}

#[allow(clippy::too_many_arguments)]
fn push(
    obs: &mut Observation,
    host: &str,
    prefix: &str,
    chip: &str,
    channel: &str,
    display: &str,
    class: &str,
    health: Health,
    fault: Option<&str>,
    metric: Option<(&str, f64)>,
) -> String {
    let channel = slug(channel);
    let mut id = if channel.is_empty() {
        format!("{prefix}:{host}:{chip}")
    } else {
        format!("{prefix}:{host}:{chip}:{channel}")
    };
    if obs.resources.iter().any(|r| r.id == id) {
        id = format!("{id}_2");
    }
    let mut resource = Resource::new(&id, ResourceKind::Sensor, display)
        .parent(format!("host:{host}"))
        .health(health)
        .label("host", host)
        .attr("class", class)
        .attr("chip", chip);
    if let Some(fault) = fault {
        resource = resource.attr("fault", fault);
    }
    obs.resources.push(resource);
    if let Some((name, value)) = metric {
        obs.samples.push(Sample::new(&id, name, value));
    }
    id
}

fn label_or<'a>(label: &'a str, fallback: &'a str) -> &'a str {
    if label.is_empty() {
        fallback
    } else {
        label
    }
}

fn psi_some_avg10(path: &Path) -> Option<f64> {
    let text = fs::read_to_string(path).ok()?;
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() != Some("some") {
            continue;
        }
        for part in parts {
            if let Some(value) = part.strip_prefix("avg10=") {
                return value.parse().ok();
            }
        }
    }
    None
}

fn read_text(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|text| text.trim().to_string())
}

fn read_number(path: &Path) -> Option<f64> {
    read_text(path)?.parse().ok()
}

fn slug(raw: &str) -> String {
    let mut out = String::new();
    let mut spaced = false;
    for c in raw.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            spaced = false;
        } else if !spaced && !out.is_empty() {
            out.push('_');
            spaced = true;
        }
    }
    let out = out.trim_matches('_').to_string();
    out
}

fn worse(a: Health, b: Health) -> Health {
    if a < b {
        a
    } else {
        b
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pulseblade-sensors-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(path: &Path, text: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, text).unwrap();
    }

    #[test]
    fn thresholds_fail_and_idle_headers_are_skipped() {
        let root = scratch();
        let cpu = root.join("class/hwmon/hwmon0");
        write(&cpu.join("name"), "k10temp\n");
        write(&cpu.join("temp1_input"), "90000\n");
        write(&cpu.join("temp1_label"), "Tctl\n");
        write(&cpu.join("temp1_crit"), "85000\n");
        write(&cpu.join("temp2_input"), "0\n");
        let fan = root.join("class/hwmon/hwmon1");
        write(&fan.join("name"), "nct6798\n");
        write(&fan.join("fan1_input"), "0\n");
        write(&fan.join("fan2_input"), "400\n");
        write(&fan.join("fan2_min"), "600\n");
        write(&fan.join("fan2_label"), "CPU Fan\n");
        let nvme = root.join("class/hwmon/hwmon2");
        write(&nvme.join("name"), "nvme\n");
        std::os::unix::fs::symlink("../nvme0", nvme.join("device")).unwrap();
        write(&nvme.join("temp1_input"), "34000\n");
        write(&nvme.join("temp1_label"), "Composite\n");
        let acpi = root.join("class/hwmon/hwmon3");
        write(&acpi.join("name"), "acpitz\n");
        write(&acpi.join("temp1_input"), "16800\n");

        let obs = collect_at("box", &root, &root.join("proc"), &mut None);
        let hot = obs
            .resources
            .iter()
            .find(|r| r.id == "temp:box:k10temp:tctl")
            .unwrap();
        assert_eq!(hot.health, Health::Failed);
        assert_eq!(hot.attrs["fault"], "thermal critical");
        assert_eq!(hot.parent.as_deref(), Some("host:box"));
        let slow = obs
            .resources
            .iter()
            .find(|r| r.id == "fan:box:nct6798:cpu_fan")
            .unwrap();
        assert_eq!(slow.health, Health::Failed);
        assert!(obs
            .resources
            .iter()
            .all(|r| !r.id.contains("fan1") && !r.id.contains("acpitz")));
        let disk = obs
            .resources
            .iter()
            .find(|r| r.id == "temp:box:nvme0:composite")
            .unwrap();
        assert_eq!(disk.health, Health::Ok);
        assert_eq!(
            obs.samples
                .iter()
                .find(|s| s.resource_id == disk.id)
                .unwrap()
                .value,
            34.0
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn stall_ecc_throttle_and_a_dead_controller_fail() {
        let root = scratch();
        write(
            &root.join("proc/pressure/memory"),
            "some avg10=55.00 avg60=1.00 avg300=0.00 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
        );
        write(
            &root.join("proc/pressure/io"),
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
        );
        write(&root.join("devices/system/edac/mc/mc0/ue_count"), "2\n");
        write(&root.join("devices/system/edac/mc/mc0/ce_count"), "4\n");
        write(
            &root.join("devices/system/cpu/cpu0/thermal_throttle/package_throttle_count"),
            "10\n",
        );
        write(&root.join("class/nvme/nvme0/state"), "live\n");
        write(&root.join("class/nvme/nvme1/state"), "connecting\n");

        let mut prev = None;
        let first = collect_at("box", &root, &root.join("proc"), &mut prev);
        assert_eq!(prev, Some(10));
        let pressure = first
            .resources
            .iter()
            .find(|r| r.id == "sensor:box:pressure")
            .unwrap();
        assert_eq!(pressure.health, Health::Failed);
        assert_eq!(pressure.attrs["fault"], "memory pressure");
        let edac = first
            .resources
            .iter()
            .find(|r| r.id == "sensor:box:edac")
            .unwrap();
        assert_eq!(edac.health, Health::Failed);
        let throttle = first
            .resources
            .iter()
            .find(|r| r.id == "sensor:box:throttle")
            .unwrap();
        assert_eq!(throttle.health, Health::Ok);
        let dead = first
            .resources
            .iter()
            .find(|r| r.id == "sensor:box:nvme1:state")
            .unwrap();
        assert_eq!(dead.health, Health::Failed);
        assert_eq!(
            first
                .resources
                .iter()
                .find(|r| r.id == "sensor:box:nvme0:state")
                .unwrap()
                .health,
            Health::Ok
        );

        write(
            &root.join("devices/system/cpu/cpu0/thermal_throttle/package_throttle_count"),
            "12\n",
        );
        let second = collect_at("box", &root, &root.join("proc"), &mut prev);
        let throttle = second
            .resources
            .iter()
            .find(|r| r.id == "sensor:box:throttle")
            .unwrap();
        assert_eq!(throttle.health, Health::Degraded);
        assert_eq!(
            second
                .samples
                .iter()
                .find(|s| s.metric == "cpu.throttle")
                .unwrap()
                .value,
            2.0
        );
        let _ = fs::remove_dir_all(&root);
    }
}
