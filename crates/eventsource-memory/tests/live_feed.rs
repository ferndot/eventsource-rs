//! Live tail of the in-memory store's global feed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::store::AggregateStore;
use eventsource_core::{AggregateId, Category, CorrelationId, ExpectedVersion, LiveFeed, StreamId};
use eventsource_memory::InMemoryEventStore;
use futures::StreamExt;
use futures::task::ArcWake;

const IDLE: Duration = Duration::from_millis(50);
/// Runtime yields given to a stream to misbehave; a spinning one wakes on every poll.
const YIELDS: usize = 1_000;
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

    // Caught up: the stream is pending and does not spin.
    //
    // This is deliberately not timing-based. The stream is polled by hand with a
    // waker that counts wake-ups. A stream that busy-loops asks to be re-polled
    // from inside its own poll (a self-wake), which shows up in the count whatever
    // the scheduler does. We then yield to the runtime many times, giving anything
    // that would wake later plenty of chances to do so, and require zero wake-ups.
    let counter = Arc::new(WakeCount::default());
    let notifier = futures::task::waker(Arc::clone(&counter));
    let mut cx = Context::from_waker(&notifier);
    assert!(live.poll_next_unpin(&mut cx).is_pending());
    for _ in 0..YIELDS {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        counter.count(),
        0,
        "an idle live stream asked to be polled again"
    );

    // It is idle, not dead: an append wakes it and the event comes through.
    append(&store, 1).await;
    for _ in 0..YIELDS {
        tokio::task::yield_now().await;
    }
    assert!(counter.count() >= 1, "an append must wake the idle stream");
    let Poll::Ready(Some(event)) = live.poll_next_unpin(&mut cx) else {
        panic!("the appended event should be ready after the wake-up");
    };
    assert_eq!(event.position.token(), 2);
}

/// Counts how often a task asks to be polled again.
#[derive(Default)]
struct WakeCount(AtomicUsize);

impl WakeCount {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl ArcWake for WakeCount {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}
