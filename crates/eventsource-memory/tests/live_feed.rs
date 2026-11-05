//! Live tail of the in-memory store's global feed.

use std::sync::Arc;
use std::time::Duration;

use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::store::AggregateStore;
use eventsource_core::{AggregateId, Category, CorrelationId, ExpectedVersion, LiveFeed, StreamId};
use eventsource_memory::InMemoryEventStore;
use futures::StreamExt;

const IDLE: Duration = Duration::from_millis(50);
const WAIT: Duration = Duration::from_secs(5);

fn record(n: u64) -> EventRecord {
    let meta = EventMeta::new(chrono::Utc::now(), CorrelationId::new());
    EventRecord::new(meta, "OrderPlaced", serde_json::json!({ "n": n }))
}

fn new_stream() -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new("Order").expect("category"),
    )
}

/// Appends `count` events to a fresh stream.
async fn append(store: &InMemoryEventStore, count: u64) {
    let events = (0..count).map(record).collect();
    store
        .append(&new_stream(), events, ExpectedVersion::Any)
        .await
        .expect("append");
}

#[tokio::test]
async fn a_fresh_live_stream_skips_history() {
    let store = Arc::new(InMemoryEventStore::new());
    append(&store, 2).await;

    let mut live = Arc::clone(&store).subscribe_live();

    assert!(tokio::time::timeout(IDLE, live.next()).await.is_err());
}

#[tokio::test]
async fn a_live_stream_yields_an_event_appended_from_another_task() {
    let store = Arc::new(InMemoryEventStore::new());
    let mut live = Arc::clone(&store).subscribe_live();

    let writer = Arc::clone(&store);
    tokio::spawn(async move { append(&writer, 1).await });

    let event = tokio::time::timeout(WAIT, live.next())
        .await
        .expect("woken by the append")
        .expect("stream open");
    assert_eq!(event.position.token(), 1);
}

#[tokio::test]
async fn several_appends_are_yielded_in_order_with_their_positions() {
    let store = Arc::new(InMemoryEventStore::new());
    append(&store, 1).await; // history
    let mut live = Arc::clone(&store).subscribe_live();

    append(&store, 3).await;
    append(&store, 2).await;

    let mut tokens = Vec::new();
    while tokens.len() < 5 {
        let event = tokio::time::timeout(WAIT, live.next())
            .await
            .expect("event arrives")
            .expect("stream open");
        tokens.push(event.position.token());
    }
    assert_eq!(tokens, [2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn dropping_one_stream_does_not_break_another_or_later_appends() {
    let store = Arc::new(InMemoryEventStore::new());
    let mut kept = Arc::clone(&store).subscribe_live();
    let abandoned = Arc::clone(&store).subscribe_live();

    append(&store, 1).await;
    drop(abandoned);
    append(&store, 1).await;

    for expected in [1, 2] {
        let event = tokio::time::timeout(WAIT, kept.next())
            .await
            .expect("event arrives")
            .expect("stream open");
        assert_eq!(event.position.token(), expected);
    }
}

#[tokio::test]
async fn an_idle_live_stream_stays_pending_after_catching_up() {
    let store = Arc::new(InMemoryEventStore::new());
    let mut live = Arc::clone(&store).subscribe_live();
    append(&store, 1).await;
    tokio::time::timeout(WAIT, live.next())
        .await
        .expect("event arrives");

    // Caught up: neither ends nor yields, and does not spin.
    let mut polls = 0_u32;
    let idle = std::future::poll_fn(|cx| {
        polls += 1;
        live.poll_next_unpin(cx).map(|_| ())
    });
    assert!(tokio::time::timeout(IDLE, idle).await.is_err());
    assert!(polls <= 3, "idle stream was polled {polls} times");
}
