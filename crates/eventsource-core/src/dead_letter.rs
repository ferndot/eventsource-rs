//! Dead-letter port: where events a subscription could not handle are parked.
//!
//! A subscription that exhausts its [`crate::subscription::RetryPolicy`] on one
//! event can park that event here and carry on, instead of blocking forever.
//! Parked events are listed and retried later.

use std::future::Future;
use std::sync::Arc;

use crate::envelope::EventEnvelope;
use crate::error::{BusError, DeadLetterError};

/// Identifies one parked entry. Stores assign these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeadLetterId(u64);

impl DeadLetterId {
    /// Wraps a raw id. Stores assign these.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// An event a subscription gave up on, with why.
#[derive(Debug, Clone, PartialEq)]
pub struct DeadLetterEntry {
    /// The subscription (checkpoint name) that failed on the event.
    pub subscription: String,
    /// The event that could not be handled.
    pub envelope: EventEnvelope,
    /// Text of the last handler failure.
    pub error: String,
    /// How many delivery attempts were made.
    pub attempts: u32,
}

/// A [`DeadLetterEntry`] as held by a store.
#[derive(Debug, Clone, PartialEq)]
pub struct ParkedEvent {
    /// The store-assigned id of this entry.
    pub id: DeadLetterId,
    /// What was parked.
    pub entry: DeadLetterEntry,
}

/// Durable storage for events that subscriptions parked.
pub trait DeadLetterStore: Send + Sync {
    /// Parks `entry`. Parking the same event for the same subscription again
    /// replaces the earlier entry's error and attempts and keeps its id.
    ///
    /// # Errors
    /// [`DeadLetterError::Backend`] if the backend fails.
    fn park(
        &self,
        entry: DeadLetterEntry,
    ) -> impl Future<Output = Result<DeadLetterId, DeadLetterError>> + Send;

    /// The entries parked for `subscription`, oldest first. Empty if there are none.
    ///
    /// # Errors
    /// [`DeadLetterError::Backend`] if the backend fails.
    fn list(
        &self,
        subscription: &str,
    ) -> impl Future<Output = Result<Vec<ParkedEvent>, DeadLetterError>> + Send;

}

impl<T: DeadLetterStore> DeadLetterStore for Arc<T> {
    fn park(
        &self,
        entry: DeadLetterEntry,
    ) -> impl Future<Output = Result<DeadLetterId, DeadLetterError>> + Send {
        (**self).park(entry)
    }

    fn list(
        &self,
        subscription: &str,
    ) -> impl Future<Output = Result<Vec<ParkedEvent>, DeadLetterError>> + Send {
        (**self).list(subscription)
    }
}

/// The store a runner has when none is configured. It refuses to park.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoDeadLetters;

impl DeadLetterStore for NoDeadLetters {
    async fn park(&self, _entry: DeadLetterEntry) -> Result<DeadLetterId, DeadLetterError> {
        Err(DeadLetterError::NotConfigured)
    }

    async fn list(&self, _subscription: &str) -> Result<Vec<ParkedEvent>, DeadLetterError> {
        Ok(Vec::new())
    }
}

/// Text for a failed publish, naming each failing handler.
pub(crate) fn describe_failure(error: &BusError) -> String {
    match error {
        BusError::HandlerFailures(failures) => failures
            .iter()
            .map(|f| format!("subscription {}: {}", f.subscription.get(), f.error))
            .collect::<Vec<_>>()
            .join("; "),
    }
}
