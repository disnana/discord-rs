//! Single-host, in-memory Identify admission for automatic Gateway recovery.
//!
//! This is NOT a shared Bot-wide scheduler. It only protects one botd process
//! from restarting a crashing real-Discord Gateway too quickly. Several botd
//! processes or hosts managing the same Bot are not coordinated, and the state
//! is lost when botd restarts. Time is injected (`Instant` arguments) so the
//! rules are testable without sleeping; this module never reads a clock.

use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

/// Minimum spacing between two Identify grants of the same Bot. Discord allows
/// one Identify per 5 seconds per shard (per max_concurrency bucket); 30
/// seconds keeps a crash loop far from that limit and leaves time for a
/// previous Gateway to be observed as gone.
pub const MIN_IDENTIFY_INTERVAL: Duration = Duration::from_secs(30);

/// Rolling window for the per-Bot Identify count.
pub const IDENTIFY_WINDOW: Duration = Duration::from_secs(60 * 60);

/// Maximum Identifies per Bot in `IDENTIFY_WINDOW`. Discord's hard limit is
/// 1000 per day (a token reset follows when exceeded); 10 per hour caps a
/// runaway restart loop at 240 per day, on the safe side of that limit, while
/// still allowing several genuine recoveries.
pub const MAX_IDENTIFIES_PER_WINDOW: u32 = 10;

/// Bound on remembered grants, so operator-driven manual starts (which are
/// recorded but never refused) cannot grow the history without limit.
const MAX_RECORDS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalIdentifyPolicy {
    pub min_interval: Duration,
    pub window: Duration,
    pub max_per_window: u32,
}

impl Default for LocalIdentifyPolicy {
    fn default() -> Self {
        Self {
            min_interval: MIN_IDENTIFY_INTERVAL,
            window: IDENTIFY_WINDOW,
            max_per_window: MAX_IDENTIFIES_PER_WINDOW,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalAdmission {
    /// Permitted and recorded.
    Granted,
    /// Too soon after the previous grant; nothing recorded. Try again later.
    Pending { retry_after: Duration },
    /// The rolling-window count is exhausted; nothing recorded. Automatic
    /// recovery must stop; `retry_after` is when the oldest grant leaves it.
    Exhausted { retry_after: Duration },
}

/// Per-Bot grant history.
#[derive(Clone, Debug)]
pub struct LocalIdentifyAdmission {
    policy: LocalIdentifyPolicy,
    grants: VecDeque<Instant>,
}

impl LocalIdentifyAdmission {
    pub fn new(policy: LocalIdentifyPolicy) -> Self {
        Self {
            policy,
            grants: VecDeque::new(),
        }
    }

    fn prune(&mut self, now: Instant) {
        while self
            .grants
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= self.policy.window)
        {
            self.grants.pop_front();
        }
    }

    /// Decide for an automatic Identify and record it when granted.
    pub fn try_acquire(&mut self, now: Instant) -> LocalAdmission {
        self.prune(now);
        if self.grants.len() >= self.policy.max_per_window as usize {
            let retry_after = self.grants.front().map_or(Duration::ZERO, |oldest| {
                (*oldest + self.policy.window).saturating_duration_since(now)
            });
            return LocalAdmission::Exhausted { retry_after };
        }
        if let Some(last) = self.grants.back() {
            let elapsed = now.saturating_duration_since(*last);
            if elapsed < self.policy.min_interval {
                return LocalAdmission::Pending {
                    retry_after: self.policy.min_interval - elapsed,
                };
            }
        }
        self.push(now);
        LocalAdmission::Granted
    }

    /// Record an Identify that was allowed by other means (initial start,
    /// explicit operator start) without refusing it, so that an immediately
    /// following automatic recovery is still spaced and counted.
    pub fn record(&mut self, now: Instant) {
        self.prune(now);
        self.push(now);
    }

    fn push(&mut self, now: Instant) {
        if self.grants.len() == MAX_RECORDS {
            self.grants.pop_front();
        }
        self.grants.push_back(now);
    }
}

/// Independent history per Bot ID.
#[derive(Clone, Debug, Default)]
pub struct LocalIdentifyRegistry {
    policy: LocalIdentifyPolicy,
    bots: HashMap<String, LocalIdentifyAdmission>,
}

impl LocalIdentifyRegistry {
    pub fn with_policy(policy: LocalIdentifyPolicy) -> Self {
        Self {
            policy,
            bots: HashMap::new(),
        }
    }

    pub fn set_policy(&mut self, policy: LocalIdentifyPolicy) {
        self.policy = policy;
        self.bots.clear();
    }

    fn bot(&mut self, bot_id: &str) -> &mut LocalIdentifyAdmission {
        let policy = self.policy;
        self.bots
            .entry(bot_id.to_owned())
            .or_insert_with(|| LocalIdentifyAdmission::new(policy))
    }

    pub fn try_acquire(&mut self, bot_id: &str, now: Instant) -> LocalAdmission {
        self.bot(bot_id).try_acquire(now)
    }

    pub fn record(&mut self, bot_id: &str, now: Instant) {
        self.bot(bot_id).record(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn first_is_granted_and_next_within_interval_is_pending_with_remaining_time() {
        let t0 = Instant::now();
        let mut a = LocalIdentifyAdmission::new(LocalIdentifyPolicy::default());
        assert_eq!(a.try_acquire(t0), LocalAdmission::Granted);
        assert_eq!(
            a.try_acquire(t0 + secs(10)),
            LocalAdmission::Pending {
                retry_after: secs(20)
            }
        );
        // Pending records nothing: the interval still counts from the grant.
        assert_eq!(a.try_acquire(t0 + secs(30)), LocalAdmission::Granted);
    }

    #[test]
    fn eleventh_identify_in_an_hour_is_exhausted_and_recovers_after_the_window() {
        let t0 = Instant::now();
        let mut a = LocalIdentifyAdmission::new(LocalIdentifyPolicy::default());
        for i in 0..10 {
            assert_eq!(a.try_acquire(t0 + secs(30 * i)), LocalAdmission::Granted);
        }
        // Oldest grant is at t0; the window ends at t0 + 3600.
        assert_eq!(
            a.try_acquire(t0 + secs(300)),
            LocalAdmission::Exhausted {
                retry_after: secs(3300)
            }
        );
        assert!(matches!(
            a.try_acquire(t0 + secs(3599)),
            LocalAdmission::Exhausted { .. }
        ));
        assert_eq!(a.try_acquire(t0 + secs(3600)), LocalAdmission::Granted);
    }

    #[test]
    fn record_counts_and_spaces_without_refusing() {
        let t0 = Instant::now();
        let mut a = LocalIdentifyAdmission::new(LocalIdentifyPolicy::default());
        a.record(t0);
        assert_eq!(
            a.try_acquire(t0 + secs(5)),
            LocalAdmission::Pending {
                retry_after: secs(25)
            }
        );
        for i in 0..20 {
            a.record(t0 + secs(i));
        }
        assert!(matches!(
            a.try_acquire(t0 + secs(100)),
            LocalAdmission::Exhausted { .. }
        ));
    }

    #[test]
    fn bots_are_independent() {
        let t0 = Instant::now();
        let mut r = LocalIdentifyRegistry::default();
        assert_eq!(r.try_acquire("a", t0), LocalAdmission::Granted);
        assert_eq!(r.try_acquire("b", t0), LocalAdmission::Granted);
        assert!(matches!(
            r.try_acquire("a", t0 + secs(1)),
            LocalAdmission::Pending { .. }
        ));
        r.record("b", t0);
        assert!(matches!(
            r.try_acquire("b", t0 + secs(1)),
            LocalAdmission::Pending { .. }
        ));
    }

    #[test]
    fn history_is_bounded() {
        let t0 = Instant::now();
        let mut a = LocalIdentifyAdmission::new(LocalIdentifyPolicy::default());
        for _ in 0..(MAX_RECORDS * 2) {
            a.record(t0);
        }
        assert_eq!(a.grants.len(), MAX_RECORDS);
    }
}
