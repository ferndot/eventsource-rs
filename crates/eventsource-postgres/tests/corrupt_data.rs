//! Bad stored data surfaces as an error, never a panic.

mod common;

use std::sync::Arc;

use eventsource_core::envelope::{FeedReadOptions, StreamReadOptions};
use eventsource_core::error::StoreError;
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{
    AggregateId, Category, DomainEvent, EventRegistry, ExpectedVersion, StreamId,
};
use eventsource_postgres::{PostgresError, PostgresEventStore};
use eventsource_testing::record;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum OrderEvent {
    OrderPlaced,
}

impl DomainEvent for OrderEvent {
    fn event_types() -> &'static [&'static str] {
        &["OrderPlaced"]
    }

    fn event_type(&self) -> &'static str {
        "OrderPlaced"
    }
}

fn registry() -> Arc<EventRegistry> {
    let mut registry = EventRegistry::new();
    registry.register::<OrderEvent>().expect("register");
    Arc::new(registry)
}

fn order_stream() -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new("Order").expect("category"),
    )
}

/// Runs raw SQL against the schema, bypassing the store and its foreign keys.
async fn tamper(schema: &str, sql: &str) {
    common::tamper(
        schema,
        &format!("SET session_replication_role = replica; {sql}"),
    )
    .await;
}

async fn open(schema: &str) -> PostgresEventStore {
    PostgresEventStore::open(&common::database_url(), schema)
        .await
        .expect("open")
}

fn assert_corrupt(error: &StoreError, column: &str) {
    let StoreError::Backend(source) = error else {
        panic!("expected a backend error, got {error:?}");
    };
    match source.downcast_ref::<PostgresError>() {
        Some(PostgresError::Corrupt { column: found, .. }) => assert_eq!(*found, column),
        other => panic!("expected corrupt {column}, got {other:?}"),
    }
}

#[tokio::test]
async fn unreadable_payload_is_a_store_error_on_every_read_path() {
    let schema = common::fresh_schema();
    let store = open(&schema).await;
    let stream = order_stream();
    store
        .append(&stream, vec![record("OrderPlaced")], ExpectedVersion::Any)
        .await
        .expect("append");

    tamper(&schema, "UPDATE events SET payload = '{not json'").await;

    let by_stream = store
        .read_stream(&stream, StreamReadOptions::default())
        .await;
    assert_corrupt(&by_stream.expect_err("stream read"), "payload");
    let feed = store.read_all(None, FeedReadOptions::default()).await;
    assert_corrupt(&feed.expect_err("feed read"), "payload");
}

#[tokio::test]
async fn malformed_ids_timestamps_and_versions_are_store_errors() {
    for (column, sql) in [
        ("event_id", "UPDATE events SET event_id = 'nope'"),
        ("stream_id", "UPDATE events SET stream_id = 'nope'"),
        ("stored_at", "UPDATE events SET stored_at = 'yesterday'"),
        ("stream_version", "UPDATE events SET stream_version = -4"),
    ] {
        let schema = common::fresh_schema();
        let store = open(&schema).await;
        store
            .append(&order_stream(), vec![record("A")], ExpectedVersion::Any)
            .await
            .expect("append");
        tamper(&schema, sql).await;
        let result = store.read_all(None, FeedReadOptions::default()).await;
        assert_corrupt(&result.expect_err(column), column);
    }
}

#[tokio::test]
async fn stored_event_type_missing_from_the_registry_is_reported() {
    let schema = common::fresh_schema();
    let stream = order_stream();
    {
        // Written by an older deployment that knew more event types.
        let store = open(&schema).await;
        store
            .append(&stream, vec![record("LegacyEvent")], ExpectedVersion::Any)
            .await
            .expect("append");
    }
    let store = open(&schema).await.with_registry(registry());

    let by_stream = store
        .read_stream(&stream, StreamReadOptions::default())
        .await;
    assert!(
        matches!(by_stream, Err(StoreError::UnregisteredEventType(ref name)) if name == "LegacyEvent"),
        "got {by_stream:?}"
    );
    let feed = store.read_all(None, FeedReadOptions::default()).await;
    assert!(matches!(feed, Err(StoreError::UnregisteredEventType(_))));
}

#[tokio::test]
async fn unregistered_event_type_is_rejected_on_append() {
    let store = open(&common::fresh_schema())
        .await
        .with_registry(registry());
    let stream = order_stream();

    let rejected = store
        .append(&stream, vec![record("NotRegistered")], ExpectedVersion::Any)
        .await;
    assert!(
        matches!(rejected, Err(StoreError::UnregisteredEventType(ref name)) if name == "NotRegistered")
    );
    store
        .append(&stream, vec![record("OrderPlaced")], ExpectedVersion::Any)
        .await
        .expect("registered type accepted");
}
