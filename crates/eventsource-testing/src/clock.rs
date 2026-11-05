//! A clock tests move by hand.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use eventsource_core::Clock;

/// A [`Clock`] that only moves when [`ManualClock::advance`] is called.
#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<DateTime<Utc>>,
}

impl ManualClock {
    /// A clock fixed at `start`.
    #[must_use]
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    /// Moves the clock forward by `by`.
    ///
    /// # Panics
    /// If `by` does not fit a `chrono::Duration`.
    pub fn advance(&self, by: Duration) {
        let by = chrono::Duration::from_std(by).expect("duration in range");
        let mut now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        *now += by;
    }
}

impl Default for ManualClock {
    /// Starts at 2024-01-01T00:00:00Z.
    fn default() -> Self {
        Self::new(
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
                .single()
                .expect("valid date"),
        )
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
