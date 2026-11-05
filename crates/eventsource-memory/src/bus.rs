//! In-process event bus: delivers committed events to handlers in this process.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use eventsource_core::bus::{EventPublisher, Handler, HandlerFailure, SubscriptionId};
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::error::BusError;
use futures::FutureExt;

use crate::background::BackgroundDelivery;

/// Turns a caught panic payload into a readable message.
fn panic_message(panic: &(dyn Any + Send)) -> String {
    let detail = panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned());
    format!("handler panicked: {detail}")
}

/// Which events a subscription receives.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Interest {
    /// Only events with this wire name.
    EventType(String),
    /// Every event.
    All,
}

struct Subscription {
    id: SubscriptionId,
    interest: Interest,
    handler: Handler,
}

/// Delivers events to subscribed handlers in this process.
///
/// Handlers for a specific event type run before catch-all handlers, and within
/// each group they run in subscription order. A failing handler does not stop
/// the others. Failures are collected and returned once every handler has run.
#[derive(Default)]
pub struct InMemoryEventBus {
    next_id: AtomicU64,
    subscriptions: RwLock<Vec<Subscription>>,
    background: Arc<BackgroundDelivery>,
}

impl InMemoryEventBus {
    /// An empty bus with no subscriptions.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribes `handler` to events with the wire name `event_type`.
    pub fn subscribe(&self, event_type: impl Into<String>, handler: Handler) -> SubscriptionId {
        self.add(Interest::EventType(event_type.into()), handler)
    }

    /// Subscribes `handler` to every event.
    pub fn subscribe_all(&self, handler: Handler) -> SubscriptionId {
        self.add(Interest::All, handler)
    }

    /// Removes one subscription. Returns `false` if it was not found.
    pub fn unsubscribe(&self, id: SubscriptionId) -> bool {
        let mut subscriptions = self.write();
        let before = subscriptions.len();
        subscriptions.retain(|s| s.id != id);
        subscriptions.len() != before
    }

    fn add(&self, interest: Interest, handler: Handler) -> SubscriptionId {
        let id = SubscriptionId::new(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.write().push(Subscription {
            id,
            interest,
            handler,
        });
        id
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<Subscription>> {
        self.subscriptions
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Vec<Subscription>> {
        self.subscriptions
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Snapshot of the handlers for one event, cloned out so no lock is held while awaiting.
    fn handlers_for(&self, event_type: &str) -> Vec<(SubscriptionId, Handler)> {
        let subscriptions = self.read();
        let specific = subscriptions
            .iter()
            .filter(|s| matches!(&s.interest, Interest::EventType(name) if name == event_type));
        let catch_all = subscriptions.iter().filter(|s| s.interest == Interest::All);
        specific
            .chain(catch_all)
            .map(|s| (s.id, s.handler.clone()))
            .collect()
    }
}

impl InMemoryEventBus {
    /// Opt-in background delivery: spawns a task that delivers `envelopes` and
    /// returns at once. Inline [`EventPublisher::publish`] stays the default.
    ///
    /// Must be called inside a Tokio runtime.
    pub fn publish_background(self: &Arc<Self>, envelopes: Vec<EventEnvelope>) {
        let bus = Arc::clone(self);
        self.background
            .spawn(async move { bus.deliver(&envelopes).await });
    }

    /// Waits until every background delivery started so far has finished.
    pub async fn flush(&self) {
        self.background.flush().await;
    }

    /// Takes the failures recorded by background deliveries.
    #[must_use]
    pub fn take_background_failures(&self) -> Vec<HandlerFailure> {
        self.background.take_failures()
    }
}

impl EventPublisher for InMemoryEventBus {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        self.deliver(envelopes).await
    }
}

impl InMemoryEventBus {
    /// Runs every handler for every envelope, isolating panics and errors.
    async fn deliver(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        let mut failures = Vec::new();
        for envelope in envelopes {
            for (subscription, handler) in self.handlers_for(&envelope.record.event_type) {
                // A panicking handler is reported as a failure, so later handlers still run.
                let outcome = AssertUnwindSafe(async { handler(envelope.clone()).await })
                    .catch_unwind()
                    .await;
                let error = match outcome {
                    Ok(Ok(())) => continue,
                    Ok(Err(error)) => error,
                    Err(panic) => panic_message(&*panic).into(),
                };
                failures.push(HandlerFailure {
                    subscription,
                    error,
                });
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(BusError::HandlerFailures(failures))
        }
    }
}
