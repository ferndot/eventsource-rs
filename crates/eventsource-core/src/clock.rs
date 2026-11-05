//! Time source, injected so tests can control timestamps.

use chrono::{DateTime, Utc};

/// A source of the current time.
pub trait Clock: Send + Sync {
    /// The current instant in UTC.
    fn now(&self) -> DateTime<Utc>;
}

/// Reads the system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
