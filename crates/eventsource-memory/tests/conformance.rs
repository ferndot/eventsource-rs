//! Store conformance and registry checks for the in-memory store.

use std::sync::Arc;

use eventsource_core::error::StoreError;
use eventsource_core::store::AggregateStore;
use eventsource_core::{
    AggregateId, Category, DomainEvent, EventRegistry, ExpectedVersion, StreamId,
};
use eventsource_memory::InMemoryEventStore;

#[tokio::test]
async fn memory_store_passes_conformance() {
    eventsource_testing::run_all(InMemoryEventStore::new).await;
}

#[tokio::test]
async fn unregistered_event_type_is_rejected() {
    let mut registry = EventRegistry::new();
    registry.register::<OrderEvent>().expect("register");
    let store = InMemoryEventStore::new().with_registry(Arc::new(registry));
    let stream = StreamId::new(
        AggregateId::new(),
        Category::new("Order").expect("category"),
    );

    let result = store
        .append(
            &stream,
            vec![eventsource_testing::record("NotRegistered")],
            ExpectedVersion::Any,
        )
        .await;
    assert!(
        matches!(result, Err(StoreError::UnregisteredEventType(name)) if name == "NotRegistered")
    );

    store
        .append(
            &stream,
            vec![eventsource_testing::record("OrderPlaced")],
            ExpectedVersion::Any,
        )
        .await
        .expect("registered type accepted");
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum OrderEvent {
    OrderPlaced { total: u64 },
}

impl DomainEvent for OrderEvent {
    fn event_types() -> &'static [&'static str] {
        &["OrderPlaced"]
    }

    fn event_type(&self) -> &'static str {
        "OrderPlaced"
    }
}

#[tokio::test]
async fn check_count_matches_the_number_of_stores_run_all_requests() {
    // Backends that must open stores up front (postgres) size their pool from
    // CHECK_COUNT, so it has to equal the number of factory calls.
    let made = std::sync::atomic::AtomicUsize::new(0);
    eventsource_testing::run_all(|| {
        made.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        InMemoryEventStore::new()
    })
    .await;
    assert_eq!(
        made.load(std::sync::atomic::Ordering::SeqCst),
        eventsource_testing::CHECK_COUNT
    );
}
