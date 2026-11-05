//! Dead-letter queue: a permanently failing event is parked and the runner moves on.

use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventsource_core::bus::handler;
use eventsource_core::dead_letter::DeadLetterStore;
use eventsource_core::error::BackendError;
use eventsource_core::store::AggregateStore;
use eventsource_core::{
    AggregateId, Category, EventEnvelope, ExpectedVersion, RetryPolicy, StreamId,
    SubscriptionConfig, SubscriptionRunner,
};
use eventsource_memory::{
    InMemoryCheckpointStore, InMemoryDeadLetterStore, InMemoryEventBus, InMemoryEventStore,
};
use eventsource_testing::record;

fn fast_retry(attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts: NonZeroU32::new(attempts).expect("nonzero"),
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
    }
}

/// Appends one event per name, each in its own stream, in order.
async fn append_events(store: &InMemoryEventStore, names: &[&str]) {
    for name in names {
        let stream = StreamId::new(AggregateId::new(), Category::new("Order").expect("category"));
        store
            .append(&stream, vec![record(name)], ExpectedVersion::NoStream)
            .await
            .expect("append");
    }
}

/// A bus whose handler records every event type it is given and fails for `poison`.
fn bus_failing_on(poison: &'static str) -> (Arc<InMemoryEventBus>, Arc<Mutex<Vec<String>>>) {
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let bus = Arc::new(InMemoryEventBus::new());
    let sink = Arc::clone(&seen);
    bus.subscribe_all(handler(move |envelope: EventEnvelope| {
        let sink = Arc::clone(&sink);
        async move {
            let name = envelope.record.event_type.clone();
            sink.lock().expect("lock").push(name.clone());
            if name == poison {
                Err::<(), BackendError>("boom".into())
            } else {
                Ok(())
            }
        }
    }));
    (bus, seen)
}

#[tokio::test]
async fn permanent_failure_is_parked_and_later_events_are_delivered() {
    let store = Arc::new(InMemoryEventStore::new());
    append_events(&store, &["First", "Poison", "Last"]).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let dead_letters = Arc::new(InMemoryDeadLetterStore::new());
    let (bus, seen) = bus_failing_on("Poison");
    let mut config = SubscriptionConfig::new("orders");
    config.retry = fast_retry(3);
    let mut runner = SubscriptionRunner::new(store, checkpoints, bus, config)
        .with_dead_letters(Arc::clone(&dead_letters));

    let stats = runner.run_until_caught_up().await.expect("runner moves on");

    let parked = dead_letters.list("orders").await.expect("list");
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].entry.subscription, "orders");
    assert_eq!(parked[0].entry.envelope.record.event_type, "Poison");
    assert_eq!(parked[0].entry.attempts, 3);
    assert!(
        parked[0].entry.error.contains("boom"),
        "error text should carry the handler's message: {}",
        parked[0].entry.error
    );
    let seen = seen.lock().expect("lock").clone();
    assert_eq!(seen.iter().filter(|n| *n == "Last").count(), 1);
    assert_eq!(seen.iter().filter(|n| *n == "Poison").count(), 3);
    assert_eq!(stats.events_delivered, 3, "parked events count as consumed");
}
