//! Snapshots: a cached copy of an aggregate's state at some stream version.
//!
//! A snapshot is only ever an optimisation. The event stream stays the source
//! of truth, so anything wrong with a snapshot means "replay the events".

use std::num::NonZeroU64;

use crate::error::SnapshotPolicyError;
use crate::version::Version;

/// When to write a snapshot: every `n` events.
///
/// A value object. It can only be built with a non-zero interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPolicy {
    every: NonZeroU64,
}

impl SnapshotPolicy {
    /// A policy that snapshots at every multiple of `n` events.
    ///
    /// # Errors
    /// [`SnapshotPolicyError::ZeroInterval`] if `n` is zero.
    pub fn every_n_events(n: u64) -> Result<Self, SnapshotPolicyError> {
        NonZeroU64::new(n)
            .map(|every| Self { every })
            .ok_or(SnapshotPolicyError::ZeroInterval)
    }

    /// The number of events between snapshots.
    #[must_use]
    pub const fn interval(self) -> u64 {
        self.every.get()
    }

    /// Whether `version` is a snapshot boundary. Version zero never is.
    #[must_use]
    pub const fn is_due(self, version: Version) -> bool {
        version.get() > 0 && version.get() % self.every.get() == 0
    }

    /// Whether a save that moved a stream from `from` to `to` stepped on or over a boundary.
    #[must_use]
    pub const fn crossed(self, from: Version, to: Version) -> bool {
        to.get() / self.every.get() > from.get() / self.every.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::Version;

    #[test]
    fn policy_rejects_a_zero_interval() {
        assert_eq!(
            SnapshotPolicy::every_n_events(0),
            Err(SnapshotPolicyError::ZeroInterval)
        );
        assert_eq!(
            SnapshotPolicy::every_n_events(5).expect("valid").interval(),
            5
        );
    }

    #[test]
    fn snapshot_is_due_at_multiples_of_the_interval_only() {
        let policy = SnapshotPolicy::every_n_events(5).expect("valid");
        let due: Vec<u64> = (0..=12)
            .filter(|v| policy.is_due(Version::new(*v)))
            .collect();
        assert_eq!(due, vec![5, 10], "version 0 is never due");

        let every = SnapshotPolicy::every_n_events(1).expect("valid");
        assert!(!every.is_due(Version::ZERO));
        assert!(every.is_due(Version::new(1)));
        assert!(every.is_due(Version::new(2)));
    }

    #[test]
    fn a_save_that_steps_over_a_boundary_is_due() {
        let policy = SnapshotPolicy::every_n_events(5).expect("valid");
        let crossed = |from, to| policy.crossed(Version::new(from), Version::new(to));
        assert!(crossed(0, 5), "landing exactly on a boundary");
        assert!(crossed(4, 7), "stepping over a boundary");
        assert!(crossed(3, 13), "stepping over two boundaries");
        assert!(!crossed(5, 7), "starting on a boundary is not crossing it");
        assert!(!crossed(6, 9));
        assert!(!crossed(7, 7), "no new events");
    }
}
