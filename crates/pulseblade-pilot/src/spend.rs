//! Spend caps across model runs. The ledger is rebuilt from the pilot's
//! recorded runs at start, so a restart does not reset a cap.

use std::time::{Duration, Instant};

use crate::config::BudgetConfig;

const HOUR: Duration = Duration::from_secs(3600);
const DAY: Duration = Duration::from_secs(24 * 3600);
const MAX_BACKOFF: Duration = Duration::from_secs(6 * 3600);

/// One recorded model run, as read back from the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendRecord {
    /// How long before now the run was recorded.
    pub age: Duration,
    pub tokens: u32,
    /// Resource ids the run looked at.
    pub subjects: Vec<String>,
    /// No endpoint answered.
    pub failed: bool,
}

#[derive(Debug, Clone)]
struct Entry {
    at: Instant,
    tokens: u32,
    subjects: Vec<String>,
    failed: bool,
}

#[derive(Debug, Default)]
pub(crate) struct Ledger {
    entries: Vec<Entry>,
}

impl Ledger {
    pub(crate) fn seed(&mut self, records: Vec<SpendRecord>, now: Instant) {
        for record in records {
            let Some(at) = now.checked_sub(record.age) else {
                continue;
            };
            self.entries.push(Entry {
                at,
                tokens: record.tokens,
                subjects: record.subjects,
                failed: record.failed,
            });
        }
        self.entries.sort_by_key(|entry| entry.at);
    }

    pub(crate) fn record(
        &mut self,
        now: Instant,
        tokens: u32,
        subjects: Vec<String>,
        failed: bool,
    ) {
        self.entries.push(Entry {
            at: now,
            tokens,
            subjects,
            failed,
        });
    }

    fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|entry| now.saturating_duration_since(entry.at) < DAY);
    }

    /// Why no model run may start now, across every resource.
    pub(crate) fn blocked(&mut self, budget: &BudgetConfig, now: Instant) -> Option<String> {
        self.prune(now);
        let failures = self
            .entries
            .iter()
            .rev()
            .take_while(|entry| entry.failed)
            .count();
        if failures > 0 && budget.failure_backoff_secs > 0 {
            let shift = u32::try_from(failures - 1).unwrap_or(u32::MAX).min(16);
            let wait = Duration::from_secs(budget.failure_backoff_secs)
                .saturating_mul(1u32 << shift)
                .min(MAX_BACKOFF);
            let last = self.entries.last().map_or(now, |entry| entry.at);
            let since = now.saturating_duration_since(last);
            if since < wait {
                return Some(format!(
                    "{failures} run(s) in a row had no endpoint answer; next try in {}s",
                    (wait - since).as_secs()
                ));
            }
        }
        let day: u64 = self
            .entries
            .iter()
            .map(|entry| u64::from(entry.tokens))
            .sum();
        if day >= u64::from(budget.max_tokens_per_day) {
            return Some(format!(
                "{day} tokens in the last 24 hours, cap {}",
                budget.max_tokens_per_day
            ));
        }
        let hour = self
            .entries
            .iter()
            .filter(|entry| now.saturating_duration_since(entry.at) < HOUR)
            .count();
        if hour >= budget.max_runs_per_hour as usize {
            return Some(format!(
                "{hour} runs in the last hour, cap {}",
                budget.max_runs_per_hour
            ));
        }
        None
    }

    /// True when this resource already had its runs for the day.
    pub(crate) fn resource_spent(
        &mut self,
        budget: &BudgetConfig,
        subject: &str,
        now: Instant,
    ) -> bool {
        self.prune(now);
        let runs = self
            .entries
            .iter()
            .filter(|entry| entry.subjects.iter().any(|s| s == subject))
            .count();
        runs >= budget.max_runs_per_resource_per_day as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_hold_across_a_seeded_restart() {
        let budget = BudgetConfig {
            max_tokens_per_day: 1000,
            max_runs_per_hour: 2,
            max_runs_per_resource_per_day: 1,
            failure_backoff_secs: 60,
            ..BudgetConfig::default()
        };
        let now = Instant::now() + DAY;
        let mut ledger = Ledger::default();
        ledger.seed(
            vec![SpendRecord {
                age: Duration::from_secs(7200),
                tokens: 400,
                subjects: vec!["host:a".into()],
                failed: false,
            }],
            now,
        );
        assert!(ledger.blocked(&budget, now).is_none());
        assert!(ledger.resource_spent(&budget, "host:a", now));
        assert!(!ledger.resource_spent(&budget, "host:b", now));

        ledger.record(now, 300, vec!["host:b".into()], false);
        ledger.record(now, 0, vec!["host:c".into()], false);
        assert!(ledger
            .blocked(&budget, now)
            .unwrap()
            .contains("runs in the last hour"));
        let later = now + HOUR;
        assert!(ledger.blocked(&budget, later).is_none());
        ledger.record(later, 400, vec![], false);
        assert!(ledger
            .blocked(&budget, later)
            .unwrap()
            .contains("tokens in the last 24 hours"));
        assert!(ledger.blocked(&budget, later + DAY).is_none());
    }

    #[test]
    fn failures_back_off_and_double() {
        let budget = BudgetConfig {
            failure_backoff_secs: 60,
            ..BudgetConfig::default()
        };
        let now = Instant::now();
        let mut ledger = Ledger::default();
        ledger.record(now, 0, vec![], true);
        assert!(ledger
            .blocked(&budget, now + Duration::from_secs(30))
            .is_some());
        assert!(ledger
            .blocked(&budget, now + Duration::from_secs(61))
            .is_none());
        let second = now + Duration::from_secs(61);
        ledger.record(second, 0, vec![], true);
        assert!(ledger
            .blocked(&budget, second + Duration::from_secs(90))
            .is_some());
        assert!(ledger
            .blocked(&budget, second + Duration::from_secs(121))
            .is_none());
        ledger.record(second + Duration::from_secs(121), 100, vec![], false);
        assert!(ledger
            .blocked(&budget, second + Duration::from_secs(122))
            .is_none());
    }
}
