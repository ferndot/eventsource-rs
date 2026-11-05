//! An event publisher that remembers what it was given.

use std::sync::{Mutex, PoisonError};

use eventsource_core::{BusError, EventEnvelope, EventPublisher, NoPublisher};

/// An [`EventPublisher`] that records every envelope it is asked to publish.
///
/// Envelopes are recorded before the wrapped publisher runs, so a publish that
/// fails in the wrapped bus is still visible to assertions: tests see what was
/// published even when delivery failed. By default nothing is delivered; use
/// [`wrapping`](Self::wrapping) to record in front of a real bus.
///
/// Recording is unbounded, which suits short-lived tests.
#[derive(Debug, Default)]
pub struct RecordingEventBus<P = NoPublisher> {
    inner: P,
    published: Mutex<Vec<EventEnvelope>>,
}

impl RecordingEventBus {
    /// A recorder that delivers nowhere.
    #[must_use]
    pub fn new() -> Self {
        Self::wrapping(NoPublisher)
    }
}

impl<P> RecordingEventBus<P> {
    /// A recorder that also forwards every publish to `inner`.
    #[must_use]
    pub const fn wrapping(inner: P) -> Self {
        Self {
            inner,
            published: Mutex::new(Vec::new()),
        }
    }

    /// The wrapped publisher.
    pub const fn inner(&self) -> &P {
        &self.inner
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<EventEnvelope>> {
        self.published
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// A copy of the recorded envelopes, in publish order.
    #[must_use]
    pub fn published(&self) -> Vec<EventEnvelope> {
        self.lock().clone()
    }

    /// The wire names of the recorded events, in publish order.
    #[must_use]
    pub fn event_types(&self) -> Vec<String> {
        self.lock()
            .iter()
            .map(|e| e.record.event_type.clone())
            .collect()
    }

    /// How many envelopes have been recorded.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether nothing has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Forgets everything recorded so far.
    pub fn clear(&self) {
        self.lock().clear();
    }
}

impl<P: EventPublisher> EventPublisher for RecordingEventBus<P> {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        self.lock().extend_from_slice(envelopes);
        self.inner.publish(envelopes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventBuilder;
    use eventsource_core::{AggregateId, Category, EventPublisher, StreamId};

    fn envelopes(types: &[&str]) -> Vec<eventsource_core::EventEnvelope> {
        let stream = StreamId::new(AggregateId::new(), Category::new("Order").expect("category"));
        types
            .iter()
            .zip(1..)
            .map(|(t, version)| EventBuilder::new(*t).build_envelope(&stream, version))
            .collect()
    }

    #[tokio::test]
    async fn records_in_publish_order_and_clears_on_request() {
        let bus = RecordingEventBus::new();
        assert!(bus.is_empty());

        bus.publish(&envelopes(&["A", "B"])).await.expect("publish");
        bus.publish(&envelopes(&["C"])).await.expect("publish");
        assert_eq!(bus.len(), 3);
        assert_eq!(bus.event_types(), ["A", "B", "C"]);
        assert_eq!(bus.published().len(), 3);

        bus.clear();
        assert!(bus.is_empty());
        assert!(bus.published().is_empty());
    }
}
