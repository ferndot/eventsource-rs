//! Projecting events into a read model through a subscription runner.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use eventsource_core::checkpoint::CheckpointStore;
use eventsource_core::error::{BackendError, ReadModelError, SubscriptionError};
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::projection::{ProjectionSink, Projector, ReadModelChange, ReadView};
use eventsource_core::readmodel::ReadModelStore;
use eventsource_core::store::AggregateStore;
use eventsource_core::{
    AggregateId, Category, CorrelationId, EventEnvelope, ExpectedVersion, RetryPolicy, StreamId,
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

// ---- Failure injection ----

/// A read model store that rejects writes while `failing` is set.
struct FlakyReadModels {
    inner: InMemoryReadModelStore,
    failing: AtomicBool,
}

impl FlakyReadModels {
    fn check(&self) -> Result<(), ReadModelError> {
        if self.failing.load(Ordering::SeqCst) {
            Err(ReadModelError::Backend("disk full".into()))
        } else {
            Ok(())
        }
    }
}

impl ReadModelStore for FlakyReadModels {
    async fn upsert(&self, model: &str, id: &str, document: Value) -> Result<(), ReadModelError> {
        self.check()?;
        self.inner.upsert(model, id, document).await
    }

    async fn get(&self, model: &str, id: &str) -> Result<Option<Value>, ReadModelError> {
        self.inner.get(model, id).await
    }

    async fn delete(&self, model: &str, id: &str) -> Result<(), ReadModelError> {
        self.check()?;
        self.inner.delete(model, id).await
    }
}

/// Counts how many events reach the wrapped projector.
struct Counting<P> {
    inner: P,
    applied: Arc<AtomicUsize>,
}

impl<P: Projector> Projector for Counting<P> {
    fn apply<S: ReadModelStore>(
        &mut self,
        envelope: &EventEnvelope,
        view: &ReadView<'_, S>,
    ) -> impl Future<Output = Result<Vec<ReadModelChange>, BackendError>> + Send {
        self.applied.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(envelope, view)
    }
}

fn single_attempt() -> SubscriptionConfig {
    let mut config = SubscriptionConfig::new("order-summaries");
    config.retry = RetryPolicy {
        max_attempts: NonZeroU32::new(1).expect("nonzero"),
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
    };
    config
}

#[tokio::test]
async fn checkpoint_does_not_advance_when_the_read_model_write_fails() {
    let events = Arc::new(InMemoryEventStore::new());
    let read_models = Arc::new(FlakyReadModels {
        inner: InMemoryReadModelStore::new(),
        failing: AtomicBool::new(true),
    });
    let checkpoints = Arc::new(InMemoryCheckpointStore::new());
    let applied = Arc::new(AtomicUsize::new(0));

    let stream = order_stream();
    let id = stream.aggregate_id().to_string();
    append(
        &events,
        &stream,
        vec![event("OrderPlaced", json!({"total": 5}))],
    )
    .await;
    append(
        &events,
        &stream,
        vec![event("OrderShipped", json!({"tracking": "T-9"}))],
    )
    .await;

    let sink = ProjectionSink::new(
        Arc::clone(&read_models),
        Counting {
            inner: OrderSummaries,
            applied: Arc::clone(&applied),
        },
    );
    let mut runner = SubscriptionRunner::new(
        Arc::clone(&events),
        Arc::clone(&checkpoints),
        sink,
        single_attempt(),
    );

    let failed = runner.run_until_caught_up().await;
    assert!(matches!(failed, Err(SubscriptionError::Handler { .. })));
    assert_eq!(
        checkpoints.load("order-summaries").await.expect("load"),
        None,
        "the checkpoint must not move past a page whose write failed"
    );
    assert_eq!(read_models.get(MODEL, &id).await.expect("get"), None);
    assert_eq!(applied.load(Ordering::SeqCst), 2);

    read_models.failing.store(false, Ordering::SeqCst);
    let stats = runner.run_until_caught_up().await.expect("second run");
    assert_eq!(stats.events_delivered, 2);
    assert_eq!(
        applied.load(Ordering::SeqCst),
        4,
        "the failed page is applied again"
    );
    assert_eq!(
        read_models.get(MODEL, &id).await.expect("get"),
        Some(json!({"status": "shipped", "total": 5, "tracking": "T-9"}))
    );
    let saved = checkpoints.load("order-summaries").await.expect("load");
    assert_eq!(saved.map(|c| c.events_processed), Some(2));
}
