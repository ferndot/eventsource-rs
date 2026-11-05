//! Dead-letter queue: a permanently failing event is parked and the runner moves on.

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventsource_core::bus::handler;
use eventsource_core::dead_letter::{DeadLetterEntry, DeadLetterStore, RetryParkedError, retry_parked};
use eventsource_core::checkpoint::CheckpointStore;
use eventsource_core::error::{BackendError, SubscriptionError};
use eventsource_core::envelope::FeedReadOptions;
use eventsource_core::store::{AggregateStore, GlobalFeed};
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

#[tokio::test]
async fn checkpoint_advances_past_the_parked_event() {
    let store = Arc::new(InMemoryEventStore::new());
    append_events(&store, &["First", "Poison", "Last"]).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let dead_letters = Arc::new(InMemoryDeadLetterStore::new());
    let (bus, _seen) = bus_failing_on("Poison");
    let mut config = SubscriptionConfig::new("orders");
    config.retry = fast_retry(2);
    SubscriptionRunner::new(
        Arc::clone(&store),
        Arc::clone(&checkpoints),
        bus,
        config.clone(),
    )
    .with_dead_letters(Arc::clone(&dead_letters))
    .run_until_caught_up()
    .await
    .expect("first run");

    let saved = checkpoints.load("orders").await.expect("load");
    assert_eq!(saved.map(|c| c.events_processed), Some(3));

    let (restart_bus, restart_seen) = bus_failing_on("Poison");
    SubscriptionRunner::new(store, checkpoints, restart_bus, config)
        .with_dead_letters(Arc::clone(&dead_letters))
        .run_until_caught_up()
        .await
        .expect("restart");
    assert!(
        restart_seen.lock().expect("lock").is_empty(),
        "nothing is redelivered after restart"
    );
    assert_eq!(dead_letters.list("orders").await.expect("list").len(), 1);
}

#[tokio::test]
async fn without_a_dead_letter_store_a_permanent_failure_stops_the_subscription() {
    let store = Arc::new(InMemoryEventStore::new());
    append_events(&store, &["First", "Poison", "Last"]).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let (bus, seen) = bus_failing_on("Poison");
    let mut config = SubscriptionConfig::new("orders");
    config.retry = fast_retry(3);
    let mut runner = SubscriptionRunner::new(store, Arc::clone(&checkpoints), bus, config);

    let result = runner.run_until_caught_up().await;

    assert!(matches!(
        result,
        Err(SubscriptionError::Handler { attempts: 3, .. })
    ));
    assert_eq!(checkpoints.load("orders").await.expect("load"), None);
    // The bus runs a whole page on each attempt, so the page is retried as a unit.
    let seen = seen.lock().expect("lock").clone();
    assert_eq!(seen.iter().filter(|n| *n == "Poison").count(), 3);
}

#[tokio::test]
async fn transient_failure_within_the_retry_budget_is_not_parked() {
    let store = Arc::new(InMemoryEventStore::new());
    append_events(&store, &["Flaky", "Last"]).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let dead_letters = Arc::new(InMemoryDeadLetterStore::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let bus = Arc::new(InMemoryEventBus::new());
    let counter = Arc::clone(&calls);
    bus.subscribe_all(handler(move |envelope: EventEnvelope| {
        let counter = Arc::clone(&counter);
        async move {
            if envelope.record.event_type == "Flaky" && counter.fetch_add(1, Ordering::SeqCst) == 0
            {
                Err::<(), BackendError>("flaky".into())
            } else {
                Ok(())
            }
        }
    }));
    let mut config = SubscriptionConfig::new("orders");
    config.retry = fast_retry(3);
    let mut runner = SubscriptionRunner::new(store, checkpoints, bus, config)
        .with_dead_letters(Arc::clone(&dead_letters));

    let stats = runner.run_until_caught_up().await.expect("run");

    assert_eq!(calls.load(Ordering::SeqCst), 2, "failed once, then retried");
    assert!(dead_letters.list("orders").await.expect("list").is_empty());
    assert_eq!(stats.events_delivered, 2);
}

/// A stored envelope of the given event type.
async fn envelope(name: &str) -> EventEnvelope {
    let store = InMemoryEventStore::new();
    append_events(&store, &[name]).await;
    store
        .read_all(None, FeedReadOptions::default())
        .await
        .expect("read")
        .remove(0)
}

async fn entry(subscription: &str, name: &str) -> DeadLetterEntry {
    DeadLetterEntry {
        subscription: subscription.to_owned(),
        envelope: envelope(name).await,
        error: "boom".to_owned(),
        attempts: 5,
    }
}

#[tokio::test]
async fn memory_store_parks_lists_by_subscription_and_removes() {
    let store = InMemoryDeadLetterStore::new();
    let a = store.park(entry("orders", "A").await).await.expect("park a");
    let b = store.park(entry("orders", "B").await).await.expect("park b");
    store
        .park(entry("billing", "C").await)
        .await
        .expect("park c");
    assert_ne!(a, b);

    let orders = store.list("orders").await.expect("list");
    let types: Vec<_> = orders
        .iter()
        .map(|p| p.entry.envelope.record.event_type.as_str())
        .collect();
    assert_eq!(types, ["A", "B"], "oldest first, only this subscription");
    assert_eq!(store.list("billing").await.expect("list").len(), 1);

    assert!(store.remove(a).await.expect("remove"));
    let left = store.list("orders").await.expect("list");
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].id, b);
    assert!(
        !store.remove(a).await.expect("remove again"),
        "removing twice reports it was already gone"
    );
}

#[tokio::test]
async fn listing_an_unknown_subscription_is_empty() {
    let store = InMemoryDeadLetterStore::new();
    store.park(entry("orders", "A").await).await.expect("park");
    assert!(store.list("nobody").await.expect("list").is_empty());
}

/// Runs a subscription over `First, Poison, Last` so that `Poison` ends up parked.
async fn park_poison() -> (Arc<InMemoryCheckpointStore>, Arc<InMemoryDeadLetterStore>) {
    let store = Arc::new(InMemoryEventStore::new());
    append_events(&store, &["First", "Poison", "Last"]).await;
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let dead_letters = Arc::new(InMemoryDeadLetterStore::new());
    let (bus, _seen) = bus_failing_on("Poison");
    let mut config = SubscriptionConfig::new("orders");
    config.retry = fast_retry(2);
    SubscriptionRunner::new(store, Arc::clone(&checkpoints), bus, config)
        .with_dead_letters(Arc::clone(&dead_letters))
        .run_until_caught_up()
        .await
        .expect("run");
    (checkpoints, dead_letters)
}

#[tokio::test]
async fn retrying_a_parked_entry_removes_it_on_success_without_moving_the_checkpoint() {
    let (checkpoints, dead_letters) = park_poison().await;
    let before = checkpoints.load("orders").await.expect("load");
    let parked = dead_letters.list("orders").await.expect("list").remove(0);
    let (healthy, seen) = bus_failing_on("never");

    let removed = retry_parked(&healthy, &dead_letters, &parked)
        .await
        .expect("retry succeeds");

    assert!(removed);
    assert_eq!(*seen.lock().expect("lock"), ["Poison"]);
    assert!(dead_letters.list("orders").await.expect("list").is_empty());
    assert_eq!(checkpoints.load("orders").await.expect("load"), before);
}

#[tokio::test]
async fn retrying_a_parked_entry_that_fails_again_leaves_it_parked() {
    let (_checkpoints, dead_letters) = park_poison().await;
    let parked = dead_letters.list("orders").await.expect("list").remove(0);
    let (still_broken, _seen) = bus_failing_on("Poison");

    let result = retry_parked(&still_broken, &dead_letters, &parked).await;

    assert!(matches!(result, Err(RetryParkedError::Handler(_))));
    assert_eq!(dead_letters.list("orders").await.expect("list").len(), 1);
}
