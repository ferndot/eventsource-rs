//! Opt-in background delivery on the in-memory bus.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventsource_core::bus::handler;
use eventsource_core::error::BackendError;
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::{Category, CorrelationId, EventEnvelope, Position, StreamId, Version};
use eventsource_memory::InMemoryEventBus;
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(5);

fn envelope(token: u64) -> EventEnvelope {
    let meta = EventMeta::new(chrono::Utc::now(), CorrelationId::new());
    let category = Category::new("Order").expect("category");
    EventEnvelope {
        record: EventRecord::new(meta, "OrderPlaced", serde_json::json!({ "n": token })),
        stream: StreamId::new(eventsource_core::AggregateId::new(), category),
        stream_version: Version::new(token),
        position: Position::new("test", token),
        stored_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn publish_background_returns_before_a_slow_handler_finishes() {
    let gate = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let bus = Arc::new(InMemoryEventBus::new());
    {
        let (gate, started, finished) = (gate.clone(), started.clone(), finished.clone());
        bus.subscribe_all(handler(move |_e: EventEnvelope| {
            let (gate, started, finished) = (gate.clone(), started.clone(), finished.clone());
            async move {
                started.notify_one();
                gate.notified().await;
                finished.store(true, Ordering::SeqCst);
                Ok::<(), BackendError>(())
            }
        }));
    }

    bus.publish_background(vec![envelope(1)]);

    // The handler is running and blocked, yet publish_background already returned.
    tokio::time::timeout(WAIT, started.notified())
        .await
        .expect("handler started");
    assert!(!finished.load(Ordering::SeqCst));

    gate.notify_one();
    tokio::time::timeout(WAIT, async {
        while !finished.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler finishes once released");
}

#[tokio::test]
async fn flush_waits_for_background_delivery_and_handlers_see_envelopes_in_order() {
    let seen: Arc<Mutex<Vec<u64>>> = Arc::default();
    let bus = Arc::new(InMemoryEventBus::new());
    let sink = Arc::clone(&seen);
    bus.subscribe_all(handler(move |e: EventEnvelope| {
        let sink = Arc::clone(&sink);
        async move {
            tokio::task::yield_now().await;
            sink.lock().expect("lock").push(e.position.token());
            Ok::<(), BackendError>(())
        }
    }));

    bus.publish_background(vec![envelope(1), envelope(2), envelope(3)]);
    tokio::time::timeout(WAIT, bus.flush())
        .await
        .expect("flushed");

    assert_eq!(*seen.lock().expect("lock"), [1, 2, 3]);
}
