//! Read models persist in the database file and project the same as in memory.

use std::future::Future;
use std::sync::Arc;

use eventsource_core::error::{BackendError, ReadModelError};
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::projection::{ProjectionSink, Projector, ReadModelChange, ReadView};
use eventsource_core::readmodel::ReadModelStore;
use eventsource_core::store::AggregateStore;
use eventsource_core::{
    AggregateId, Category, CorrelationId, EventEnvelope, ExpectedVersion, StreamId,
    SubscriptionConfig, SubscriptionRunner,
};
use eventsource_memory::{InMemoryCheckpointStore, InMemoryEventStore, InMemoryReadModelStore};
use eventsource_sqlite::{
    SqliteCheckpointStore, SqliteError, SqliteEventStore, SqliteReadModelStore,
};
use serde_json::{Value, json};

const MODEL: &str = "order_summaries";

#[tokio::test]
async fn documents_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rm.db");
    {
        let store = SqliteReadModelStore::open(&path).expect("open");
        assert_eq!(store.get("orders", "1").await.expect("get"), None);
        store
            .upsert("orders", "1", json!({"status": "placed"}))
            .await
            .expect("upsert");
        store
            .upsert("orders", "1", json!({"status": "shipped"}))
            .await
            .expect("replace");
        store
            .upsert("invoices", "1", json!("inv"))
            .await
            .expect("other model");
        store
            .upsert("orders", "2", json!("gone"))
            .await
            .expect("upsert");
        store.delete("orders", "2").await.expect("delete");
        store.delete("orders", "2").await.expect("delete missing");
    }
    let reopened = SqliteReadModelStore::open(&path).expect("reopen");
    assert_eq!(
        reopened.get("orders", "1").await.expect("get"),
        Some(json!({"status": "shipped"}))
    );
    assert_eq!(
        reopened.get("invoices", "1").await.expect("get"),
        Some(json!("inv")),
        "models are isolated"
    );
    assert_eq!(reopened.get("orders", "2").await.expect("get"), None);
}

#[tokio::test]
async fn a_failing_change_rolls_back_the_whole_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rm.db");
    let store = SqliteReadModelStore::open(&path).expect("open");
    // Make the database reject one id, standing in for a write that fails mid-page.
    rusqlite::Connection::open(&path)
        .expect("second connection")
        .execute_batch(
            "CREATE TRIGGER reject_bad BEFORE INSERT ON readmodels WHEN NEW.id = 'bad' \
             BEGIN SELECT RAISE(ABORT, 'rejected'); END;",
        )
        .expect("trigger");

    let result = store
        .apply_changes(&[
            ReadModelChange::upsert("orders", "good", json!(1)),
            ReadModelChange::upsert("orders", "bad", json!(2)),
        ])
        .await;
    // The failure is the database's own rejection, surfaced as a backend error.
    let Err(ReadModelError::Backend(source)) = result else {
        panic!("expected a backend error, got {result:?}");
    };
    let Some(SqliteError::Sqlite(cause)) = source.downcast_ref::<SqliteError>() else {
        panic!("expected a sqlite failure, got {source:?}");
    };
    assert!(
        cause.to_string().contains("rejected"),
        "the trigger's message must reach the caller: {cause}"
    );
    assert_eq!(
        store.get("orders", "good").await.expect("get"),
        None,
        "the first change of the failed batch must not remain"
    );
}

// ---- A projection over SQLite matches the same projection over memory. ----

struct OrderSummaries;

impl Projector for OrderSummaries {
    fn apply<S: ReadModelStore>(
        &mut self,
        envelope: &EventEnvelope,
        view: &ReadView<'_, S>,
    ) -> impl Future<Output = Result<Vec<ReadModelChange>, BackendError>> + Send {
        let id = envelope.stream.aggregate_id().to_string();
        let payload = envelope.record.payload.clone();
        let event_type = envelope.record.event_type.clone();
        async move {
            let current = view.get(MODEL, &id).await?;
            let document = match event_type.as_str() {
                "OrderPlaced" => json!({"status": "placed", "total": payload["total"]}),
                "OrderShipped" => {
                    let mut doc = current.unwrap_or_else(|| json!({}));
                    doc["status"] = json!("shipped");
                    doc["tracking"] = payload["tracking"].clone();
                    doc
                }
                "OrderCancelled" => return Ok(vec![ReadModelChange::delete(MODEL, id)]),
                _ => return Ok(vec![]),
            };
            Ok(vec![ReadModelChange::upsert(MODEL, id, document)])
        }
    }
}

fn event(event_type: &str, payload: Value) -> EventRecord {
    EventRecord::new(
        EventMeta::new(chrono::Utc::now(), CorrelationId::new()),
        event_type,
        payload,
    )
}

/// Appends the same script to `store`, returning the order ids as (shipped, cancelled).
async fn script<S: AggregateStore>(store: &S) -> (String, String) {
    let new_stream = || {
        StreamId::new(
            AggregateId::new(),
            Category::new("Order").expect("category"),
        )
    };
    let (shipped, cancelled) = (new_stream(), new_stream());
    let steps = [
        (&shipped, event("OrderPlaced", json!({"total": 10}))),
        (&cancelled, event("OrderPlaced", json!({"total": 20}))),
        (&shipped, event("OrderShipped", json!({"tracking": "T-1"}))),
        (&cancelled, event("OrderCancelled", json!({}))),
    ];
    for (stream, record) in steps {
        store
            .append(stream, vec![record], ExpectedVersion::Any)
            .await
            .expect("append");
    }
    (
        shipped.aggregate_id().to_string(),
        cancelled.aggregate_id().to_string(),
    )
}

#[tokio::test]
async fn sqlite_projection_matches_the_in_memory_projection() {
    // In memory.
    let mem_events = Arc::new(InMemoryEventStore::new());
    let mem_models = Arc::new(InMemoryReadModelStore::new());
    let (mem_shipped, mem_cancelled) = script(&*mem_events).await;
    SubscriptionRunner::new(
        Arc::clone(&mem_events),
        Arc::new(InMemoryCheckpointStore::new()),
        ProjectionSink::new(Arc::clone(&mem_models), OrderSummaries),
        SubscriptionConfig::new("order-summaries"),
    )
    .run_until_caught_up()
    .await
    .expect("memory run");

    // On one SQLite file: events, checkpoints and read models together.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("all.db");
    let events = Arc::new(SqliteEventStore::open(&path).expect("events"));
    let models = Arc::new(SqliteReadModelStore::open(&path).expect("models"));
    let checkpoints = Arc::new(SqliteCheckpointStore::open(&path).expect("checkpoints"));
    let (shipped, cancelled) = script(&*events).await;
    let stats = SubscriptionRunner::new(
        Arc::clone(&events),
        Arc::clone(&checkpoints),
        ProjectionSink::new(Arc::clone(&models), OrderSummaries),
        SubscriptionConfig::new("order-summaries"),
    )
    .run_until_caught_up()
    .await
    .expect("sqlite run");
    assert_eq!(stats.events_delivered, 4);

    let expected = json!({"status": "shipped", "total": 10, "tracking": "T-1"});
    assert_eq!(
        models.get(MODEL, &shipped).await.expect("get"),
        Some(expected.clone())
    );
    assert_eq!(models.get(MODEL, &cancelled).await.expect("get"), None);
    assert_eq!(
        models.get(MODEL, &shipped).await.expect("get"),
        mem_models.get(MODEL, &mem_shipped).await.expect("get")
    );
    assert_eq!(
        models.get(MODEL, &cancelled).await.expect("get"),
        mem_models.get(MODEL, &mem_cancelled).await.expect("get")
    );
}
