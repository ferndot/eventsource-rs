//! Stream versions and optimistic-concurrency expectations.

use serde::{Deserialize, Serialize};

/// Number of events in a stream. `Version::ZERO` is a stream with no events.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Version(u64);

impl Version {
    /// A stream with no events.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw event count.
    #[must_use]
    pub const fn new(count: u64) -> Self {
        Self(count)
    }

    /// The raw event count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The version after one more event.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// The version after `count` more events.
    #[must_use]
    pub const fn advance(self, count: u64) -> Self {
        Self(self.0 + count)
    }
}

/// What a writer expects the stream to look like when it appends.
///
/// Passing anything other than `Any` turns an append into a compare-and-set:
/// it succeeds only if the stream is in the expected state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// No check.
    Any,
    /// The stream must be empty.
    NoStream,
    /// The stream must contain at least one event.
    StreamExists,
    /// The stream must contain exactly this many events.
    Exact(Version),
}

impl ExpectedVersion {
    /// Whether a stream currently at `current` satisfies this expectation.
    #[must_use]
    pub fn is_satisfied_by(self, current: Version) -> bool {
        match self {
            Self::Any => true,
            Self::NoStream => current == Version::ZERO,
            Self::StreamExists => current > Version::ZERO,
            Self::Exact(expected) => expected == current,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_counts_events() {
        assert_eq!(Version::ZERO.get(), 0);
        assert_eq!(Version::default(), Version::ZERO);
        assert_eq!(Version::ZERO.next(), Version::new(1));
        assert_eq!(Version::new(3).advance(4), Version::new(7));
        assert_eq!(Version::new(3).advance(0), Version::new(3));
        assert!(Version::new(2) < Version::new(3));
    }

    #[test]
    fn expected_version_any_accepts_everything() {
        assert!(ExpectedVersion::Any.is_satisfied_by(Version::ZERO));
        assert!(ExpectedVersion::Any.is_satisfied_by(Version::new(9)));
    }

    #[test]
    fn expected_no_stream_requires_empty() {
        assert!(ExpectedVersion::NoStream.is_satisfied_by(Version::ZERO));
        assert!(!ExpectedVersion::NoStream.is_satisfied_by(Version::new(1)));
    }

    #[test]
    fn expected_stream_exists_requires_events() {
        assert!(!ExpectedVersion::StreamExists.is_satisfied_by(Version::ZERO));
        assert!(ExpectedVersion::StreamExists.is_satisfied_by(Version::new(1)));
    }

    #[test]
    fn expected_exact_requires_equal_version() {
        let expected = ExpectedVersion::Exact(Version::new(2));
        assert!(expected.is_satisfied_by(Version::new(2)));
        assert!(!expected.is_satisfied_by(Version::new(1)));
        assert!(!expected.is_satisfied_by(Version::new(3)));
        assert!(ExpectedVersion::Exact(Version::ZERO).is_satisfied_by(Version::ZERO));
    }
}
