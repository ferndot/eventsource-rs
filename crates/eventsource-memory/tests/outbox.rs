//! Transactional outbox over in-memory adapters.

use eventsource_core::outbox::{NewOutboxMessage, OutboxStore};
use eventsource_memory::InMemoryOutboxStore;
use serde_json::json;

fn message(destination: &str, n: u64) -> NewOutboxMessage {
    NewOutboxMessage::new(destination, json!({ "n": n }))
}

#[tokio::test]
async fn fetch_pending_returns_enqueue_order_and_honours_limit() {
    let outbox = InMemoryOutboxStore::new();
    for n in 0..5 {
        outbox
            .enqueue(message("orders", n))
            .await
            .expect("enqueue");
    }

    let all = outbox.fetch_pending(10).await.expect("fetch");
    let order: Vec<_> = all.iter().map(|m| m.payload["n"].as_u64()).collect();
    assert_eq!(order, [Some(0), Some(1), Some(2), Some(3), Some(4)]);

    let limited = outbox.fetch_pending(2).await.expect("fetch");
    let order: Vec<_> = limited.iter().map(|m| m.payload["n"].as_u64()).collect();
    assert_eq!(order, [Some(0), Some(1)]);
    assert!(limited.iter().all(|m| m.attempts == 0 && m.destination == "orders"));
}
