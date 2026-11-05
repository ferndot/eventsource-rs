//! Projecting events into a read model through a subscription runner.

use std::future::Future;
use std::sync::Arc;

use eventsource_core::error::BackendError;
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::projection::{ProjectionSink, Projector, ReadModelChange, ReadView};
use eventsource_core::readmodel::ReadModelStore;
use eventsource_core::store::AggregateStore;
use eventsource_core::{
    AggregateId, Category, CorrelationId, EventEnvelope, ExpectedVersion, StreamId,
    SubscriptionConfig, SubscriptionRunner,
};
use eventsource_memory::{InMemoryCheckpointStore, InMemoryEventStore, InMemoryReadModelStore};
use serde_json::{Value, json};

const MODEL: &str = "order_summaries";

/// Builds one `OrderSummaries` document per order from placed/shipped events.
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
                _ => return Ok(vec![]),
            };
            Ok(vec![ReadModelChange::upsert(MODEL, id, document)])
        }
    }
}

fn order_stream() -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new("Order").expect("category"),
    )
}

fn event(event_type: &str, payload: Value) -> EventRecord {
    EventRecord::new(
        EventMeta::new(chrono::Utc::now(), CorrelationId::new()),
        event_type,
        payload,
    )
}

async fn append(store: &InMemoryEventStore, stream: &StreamId, events: Vec<EventRecord>) {
    store
        .append(stream, events, ExpectedVersion::Any)
        .await
        .expect("append");
}

#[tokio::test]
async fn projector_builds_a_document_per_order() {
    let events = Arc::new(InMemoryEventStore::new());
    let read_models = Arc::new(InMemoryReadModelStore::new());
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());

    let (a, b) = (order_stream(), order_stream());
    append(
        &events,
        &a,
        vec![event("OrderPlaced", json!({"total": 10}))],
    )
    .await;
    append(
        &events,
        &b,
        vec![event("OrderPlaced", json!({"total": 20}))],
    )
    .await;
    append(
        &events,
        &a,
        vec![event("OrderShipped", json!({"tracking": "T-1"}))],
    )
    .await;

    let sink = ProjectionSink::new(Arc::clone(&read_models), OrderSummaries);
    let mut runner = SubscriptionRunner::new(
        Arc::clone(&events),
        Arc::clone(&checkpoints),
        sink,
        SubscriptionConfig::new("order-summaries"),
    );
    let stats = runner.run_until_caught_up().await.expect("run");
    assert_eq!(stats.events_delivered, 3);

    let a_doc = read_models
        .get(MODEL, &a.aggregate_id().to_string())
        .await
        .expect("get");
    assert_eq!(
        a_doc,
        Some(json!({"status": "shipped", "total": 10, "tracking": "T-1"}))
    );
    let b_doc = read_models
        .get(MODEL, &b.aggregate_id().to_string())
        .await
        .expect("get");
    assert_eq!(b_doc, Some(json!({"status": "placed", "total": 20})));
}
