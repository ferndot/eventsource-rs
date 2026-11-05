//! Transactional outbox over in-memory adapters.

use std::sync::Mutex;

use eventsource_core::error::BackendError;
use eventsource_core::outbox::{
    NewOutboxMessage, OutboxMessage, OutboxPublisher, OutboxStore, relay,
};
use eventsource_memory::InMemoryOutboxStore;
use serde_json::json;

/// Records what it was asked to publish.
#[derive(Default)]
struct RecordingPublisher {
    seen: Mutex<Vec<u64>>,
}

impl RecordingPublisher {
    fn seen(&self) -> Vec<u64> {
        self.seen.lock().expect("lock").clone()
    }
}

impl OutboxPublisher for RecordingPublisher {
    async fn publish(&self, message: &OutboxMessage) -> Result<(), BackendError> {
        let n = message.payload["n"].as_u64().expect("n");
        self.seen.lock().expect("lock").push(n);
        Ok(())
    }
}

fn message(destination: &str, n: u64) -> NewOutboxMessage {
    NewOutboxMessage::new(destination, json!({ "n": n }))
}

#[tokio::test]
async fn fetch_pending_returns_enqueue_order_and_honours_limit() {
    let outbox = InMemoryOutboxStore::new();
    for n in 0..5 {
        outbox.enqueue(message("orders", n)).await.expect("enqueue");
    }

    let all = outbox.fetch_pending(10).await.expect("fetch");
    let order: Vec<_> = all.iter().map(|m| m.payload["n"].as_u64()).collect();
    assert_eq!(order, [Some(0), Some(1), Some(2), Some(3), Some(4)]);

    let limited = outbox.fetch_pending(2).await.expect("fetch");
    let order: Vec<_> = limited.iter().map(|m| m.payload["n"].as_u64()).collect();
    assert_eq!(order, [Some(0), Some(1)]);
    assert!(
        limited
            .iter()
            .all(|m| m.attempts == 0 && m.destination == "orders")
    );
}

#[tokio::test]
async fn relay_publishes_pending_and_marks_them_published() {
    let outbox = InMemoryOutboxStore::new();
    for n in 0..3 {
        outbox.enqueue(message("orders", n)).await.expect("enqueue");
    }
    let publisher = RecordingPublisher::default();

    let first = relay(&outbox, &publisher, 10).await.expect("relay");
    assert_eq!(first.published, 3);
    assert!(first.failed.is_empty());
    assert_eq!(publisher.seen(), [0, 1, 2]);
    assert!(outbox.fetch_pending(10).await.expect("fetch").is_empty());

    let second = relay(&outbox, &publisher, 10).await.expect("relay again");
    assert_eq!(second.published, 0);
    assert_eq!(publisher.seen(), [0, 1, 2], "nothing published twice");
}
