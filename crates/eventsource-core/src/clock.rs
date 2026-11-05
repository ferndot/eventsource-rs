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

impl<C: Clock + ?Sized> Clock for std::sync::Arc<C> {
    fn now(&self) -> DateTime<Utc> {
        (**self).now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_is_close_to_utc_now() {
        let before = Utc::now();
        let read = SystemClock.now();
        let after = Utc::now();
        let slack = chrono::Duration::seconds(5);
        assert!(read >= before - slack && read <= after + slack, "{read} not near {before}");
    }

    #[test]
    fn system_clock_does_not_go_backwards_between_reads() {
        let first = SystemClock.now();
        let second = SystemClock.now();
        assert!(second >= first - chrono::Duration::seconds(5));
    }

    #[test]
    fn arc_clock_delegates() {
        let clock: std::sync::Arc<dyn Clock> = std::sync::Arc::new(SystemClock);
        assert!((clock.now() - Utc::now()).num_seconds().abs() < 5);
    }
}
