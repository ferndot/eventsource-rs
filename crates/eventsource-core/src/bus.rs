//! Publishing ports and handler types for the event bus.

use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::envelope::EventEnvelope;
use crate::error::{BackendError, BusError};

/// Identifies one subscription on a bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    /// Wraps a raw id. Buses assign these.
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

/// One handler's failure during a publish.
#[derive(Debug)]
pub struct HandlerFailure {
    /// The subscription whose handler failed.
    pub subscription: SubscriptionId,
    /// What the handler returned.
    pub error: BackendError,
}

/// A type-erased event handler.
///
/// Handlers are stored in a collection that holds many different closures, so
/// they need runtime dispatch. The boxed future is the cost of that. It is paid
/// once per handler call and nowhere else.
pub type Handler =
    Arc<dyn Fn(EventEnvelope) -> BoxFuture<'static, Result<(), BackendError>> + Send + Sync>;

/// Wraps an async closure as a [`Handler`].
pub fn handler<F, Fut>(f: F) -> Handler
where
    F: Fn(EventEnvelope) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BackendError>> + Send + 'static,
{
    Arc::new(move |envelope| Box::pin(f(envelope)))
}

/// Delivers events to handlers. A [`crate::subscription::SubscriptionRunner`] is its only caller.
///
/// Implementations must run handlers one after another, in the order given.
pub trait EventPublisher: Send + Sync {
    /// Delivers `envelopes` to subscribers, in order.
    ///
    /// # Errors
    /// [`BusError::HandlerFailures`] if any handler failed. Every handler still runs.
    fn publish(
        &self,
        envelopes: &[EventEnvelope],
    ) -> impl Future<Output = Result<(), BusError>> + Send;
}

impl<T: EventPublisher> EventPublisher for Arc<T> {
    fn publish(
        &self,
        envelopes: &[EventEnvelope],
    ) -> impl Future<Output = Result<(), BusError>> + Send {
        (**self).publish(envelopes)
    }
}

/// A publisher that discards events. Useful as a sink when delivery is not needed.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoPublisher;

impl EventPublisher for NoPublisher {
    async fn publish(&self, _envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        Ok(())
    }
}
