//! Transactional outbox over in-memory adapters.

use std::sync::{Arc, Mutex};

use eventsource_core::envelope::StreamReadOptions;
use eventsource_core::error::{BackendError, OutboxError, RepositoryError};
use eventsource_core::outbox::{
    NewOutboxMessage, OutboxMessage, OutboxPublisher, OutboxStore, relay,
};
use eventsource_core::store::AggregateStore;
use eventsource_core::{AggregateId, AggregateRepository, CommandContext, Version};
use eventsource_memory::{InMemoryEventStore, InMemoryOutboxStore};
use eventsource_testing::fixtures::{CapturingPublisher, Counter, stream_for as counter_stream};
use serde_json::json;

/// Records what it was asked to publish.
#[derive(Default)]
struct RecordingPublisher {
    seen: Mutex<Vec<u64>>,
    fail_on: Option<u64>,
}

impl RecordingPublisher {
    /// A publisher that rejects the message numbered `n`.
    fn failing_on(n: u64) -> Self {
        Self {
            fail_on: Some(n),
            ..Self::default()
        }
    }

    fn seen(&self) -> Vec<u64> {
        self.seen.lock().expect("lock").clone()
    }
}

impl OutboxPublisher for RecordingPublisher {
    async fn publish(&self, message: &OutboxMessage) -> Result<(), BackendError> {
        let n = message.payload["n"].as_u64().expect("n");
        if self.fail_on == Some(n) {
            return Err("bus unavailable".into());
        }
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

#[tokio::test]
async fn publisher_failure_keeps_message_pending_with_attempt_counted() {
    let outbox = InMemoryOutboxStore::new();
    let ids = [
        outbox.enqueue(message("orders", 0)).await.expect("enqueue"),
        outbox.enqueue(message("orders", 1)).await.expect("enqueue"),
        outbox.enqueue(message("orders", 2)).await.expect("enqueue"),
    ];

    let report = relay(&outbox, &RecordingPublisher::failing_on(1), 10)
        .await
        .expect("relay itself succeeds");
    assert_eq!(report.published, 1);
    assert_eq!(report.failed.len(), 1);
    assert_eq!(report.failed[0].id, ids[1]);
    assert!(report.failed[0].error.contains("bus unavailable"));

    let pending = outbox.fetch_pending(10).await.expect("fetch");
    let state: Vec<_> = pending.iter().map(|m| (m.id, m.attempts)).collect();
    assert_eq!(
        state,
        [(ids[1], 1), (ids[2], 0)],
        "failed one kept, order kept, later one untouched"
    );

    // Once the bus is back the same messages go out, in order.
    let publisher = RecordingPublisher::default();
    let report = relay(&outbox, &publisher, 10).await.expect("relay");
    assert_eq!(report.published, 2);
    assert_eq!(publisher.seen(), [1, 2]);
}

#[tokio::test]
async fn failed_enqueue_leaves_the_event_stream_unchanged() {
    let outbox = InMemoryOutboxStore::new();
    let events = InMemoryEventStore::new().with_outbox(outbox.clone());
    let repo = AggregateRepository::<Counter, _>::new(events).expect("repo");
    let id = AggregateId::new();

    // An empty destination is rejected by the outbox, after the events were prepared.
    let result = repo
        .execute_with_outbox(id, vec![5], &CommandContext::new(), |_| {
            NewOutboxMessage::new("", json!({}))
        })
        .await;

    assert!(matches!(
        result,
        Err(RepositoryError::Outbox(OutboxError::Invalid(_)))
    ));
    assert_eq!(
        repo.stored_version(id).await.expect("version"),
        Version::ZERO
    );
    assert!(outbox.fetch_pending(10).await.expect("fetch").is_empty());
}

#[tokio::test]
async fn atomic_append_stores_event_and_message_and_relay_delivers_it() {
    let outbox = InMemoryOutboxStore::new();
    let events = Arc::new(InMemoryEventStore::new().with_outbox(outbox.clone()));
    let repo = AggregateRepository::<Counter, _>::new(events.clone()).expect("repo");
    let id = AggregateId::new();

    let appended = repo
        .execute_with_outbox(id, vec![5], &CommandContext::new(), |record| {
            NewOutboxMessage::new("counter-events", record.payload.clone())
        })
        .await
        .expect("execute")
        .expect("something was saved");
    assert_eq!(appended.new_version, Version::new(1));

    let stored = events
        .read_stream(&counter_stream(id), StreamReadOptions::default())
        .await
        .expect("read");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].record.event_type, "Added");

    let pending = outbox.fetch_pending(10).await.expect("fetch");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].destination, "counter-events");
    assert_eq!(pending[0].payload, stored[0].record.payload);
    assert_eq!(pending[0].stream, Some(counter_stream(id)));

    let seen = Arc::new(Mutex::new(Vec::new()));
    let report = relay(&outbox, &CapturingPublisher(seen.clone()), 10)
        .await
        .expect("relay");
    assert_eq!(report.published, 1);
    assert_eq!(
        *seen.lock().expect("lock"),
        [stored[0].record.payload.clone()]
    );
    assert!(outbox.fetch_pending(10).await.expect("fetch").is_empty());
}
