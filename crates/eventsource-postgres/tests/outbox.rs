//! The outbox persists in the schema and survives a restart.

mod common;

use std::sync::{Arc, Mutex};

use eventsource_core::envelope::StreamReadOptions;
use eventsource_core::error::{BackendError, OutboxError, RepositoryError};
use eventsource_core::outbox::{
    NewOutboxMessage, OutboxMessage, OutboxPublisher, OutboxStore, relay,
};
use eventsource_core::store::AggregateStore;
use eventsource_core::{
    AggregateId, AggregateRepository, Category, CommandContext, Decider, DomainEvent, StreamId,
    Version,
};
use eventsource_postgres::{PostgresEventStore, PostgresOutboxStore};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum CounterEvent {
    Added(u64),
}

impl DomainEvent for CounterEvent {
    fn event_types() -> &'static [&'static str] {
        &["Added"]
    }

    fn event_type(&self) -> &'static str {
        "Added"
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CounterState {
    sum: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("never rejected")]
struct Never;

struct Counter;

impl Decider for Counter {
    type Command = u64;
    type Event = CounterEvent;
    type State = CounterState;
    type Error = Never;

    const AGGREGATE_TYPE: &'static str = "Counter";

    fn initial_state() -> CounterState {
        CounterState { sum: 0 }
    }

    fn decide(_: &CounterState, amount: u64) -> Result<Vec<CounterEvent>, Never> {
        Ok(vec![CounterEvent::Added(amount)])
    }

    fn evolve(state: &CounterState, event: &CounterEvent) -> CounterState {
        let CounterEvent::Added(n) = event;
        CounterState { sum: state.sum + n }
    }
}

fn counter_stream(id: AggregateId) -> StreamId {
    StreamId::new(id, Category::new("Counter").expect("category"))
}

struct CapturingPublisher(Arc<Mutex<Vec<serde_json::Value>>>);

impl OutboxPublisher for CapturingPublisher {
    async fn publish(&self, message: &OutboxMessage) -> Result<(), BackendError> {
        self.0.lock().expect("lock").push(message.payload.clone());
        Ok(())
    }
}

async fn events(schema: &str) -> PostgresEventStore {
    PostgresEventStore::open(&common::database_url(), schema)
        .await
        .expect("open events")
}

async fn outbox_store(schema: &str) -> PostgresOutboxStore {
    PostgresOutboxStore::open(&common::database_url(), schema)
        .await
        .expect("open outbox")
}

#[tokio::test]
async fn pending_messages_survive_reopen_and_are_delivered_after_it() {
    let schema = common::fresh_schema();
    {
        let outbox = outbox_store(&schema).await;
        for n in 0..3 {
            outbox
                .enqueue(NewOutboxMessage::new("orders", json!({ "n": n })))
                .await
                .expect("enqueue");
        }
        // The process "crashes" here: nothing was published.
    }

    let outbox = outbox_store(&schema).await;
    let pending = outbox.fetch_pending(10).await.expect("fetch");
    assert_eq!(pending.len(), 3);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let report = relay(&outbox, &CapturingPublisher(seen.clone()), 10)
        .await
        .expect("relay");
    assert_eq!(report.published, 3);
    assert_eq!(
        *seen.lock().expect("lock"),
        [json!({"n": 0}), json!({"n": 1}), json!({"n": 2})]
    );
    drop(outbox);

    let outbox = outbox_store(&schema).await;
    assert!(outbox.fetch_pending(10).await.expect("fetch").is_empty());
}

#[tokio::test]
async fn failed_attempts_persist_and_the_message_stays_pending() {
    let schema = common::fresh_schema();
    let id = {
        let outbox = outbox_store(&schema).await;
        let id = outbox
            .enqueue(NewOutboxMessage::new("orders", json!({})))
            .await
            .expect("enqueue");
        outbox.mark_failed(id, "bus down").await.expect("mark");
        outbox.mark_failed(id, "bus down").await.expect("mark");
        id
    };

    let outbox = outbox_store(&schema).await;
    let pending = outbox.fetch_pending(10).await.expect("fetch");
    assert_eq!(pending.len(), 1);
    assert_eq!((pending[0].id, pending[0].attempts), (id, 2));
}

#[tokio::test]
async fn failed_enqueue_rolls_back_the_appended_event() {
    let schema = common::fresh_schema();
    let repo = AggregateRepository::<Counter, _>::new(events(&schema).await).expect("repo");
    let outbox = outbox_store(&schema).await;
    let id = AggregateId::new();

    let result = repo
        .execute_with_outbox(id, 5, &CommandContext::new(), |_| {
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
async fn atomic_append_survives_reopen_and_relay_delivers_after_it() {
    let schema = common::fresh_schema();
    let id = AggregateId::new();
    {
        let repo = AggregateRepository::<Counter, _>::new(events(&schema).await).expect("repo");
        repo.execute_with_outbox(id, 7, &CommandContext::new(), |record| {
            NewOutboxMessage::new("counter-events", record.payload.clone())
        })
        .await
        .expect("execute");
        // Crash before any relay ran.
    }

    let events = events(&schema).await;
    let stored = events
        .read_stream(&counter_stream(id), StreamReadOptions::default())
        .await
        .expect("read");
    assert_eq!(stored.len(), 1);

    let outbox = outbox_store(&schema).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let report = relay(&outbox, &CapturingPublisher(seen.clone()), 10)
        .await
        .expect("relay");
    assert_eq!(report.published, 1);
    assert_eq!(
        *seen.lock().expect("lock"),
        [stored[0].record.payload.clone()]
    );
}
