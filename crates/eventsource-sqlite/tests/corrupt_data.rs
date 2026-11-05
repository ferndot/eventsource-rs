//! Bad stored data surfaces as an error, never a panic.

use std::path::Path;
use std::sync::Arc;

use eventsource_core::envelope::{FeedReadOptions, StreamReadOptions};
use eventsource_core::error::StoreError;
use eventsource_core::store::{AggregateStore, GlobalFeed};
use eventsource_core::{
    AggregateId, Category, DomainEvent, EventRegistry, ExpectedVersion, StreamId,
};
use eventsource_sqlite::{SqliteError, SqliteEventStore};
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

/// Runs raw SQL against the file, bypassing the store.
fn tamper(path: &Path, sql: &str) {
    let conn = rusqlite::Connection::open(path).expect("raw open");
    conn.pragma_update(None, "foreign_keys", false)
        .expect("pragma");
    conn.execute_batch(sql).expect("tamper");
}

fn assert_corrupt(error: &StoreError, column: &str) {
    let StoreError::Backend(source) = error else {
        panic!("expected a backend error, got {error:?}");
    };
    match source.downcast_ref::<SqliteError>() {
        Some(SqliteError::Corrupt { column: found, .. }) => assert_eq!(*found, column),
        other => panic!("expected corrupt {column}, got {other:?}"),
    }
}

#[tokio::test]
async fn unreadable_payload_is_a_store_error_on_every_read_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    let store = SqliteEventStore::open(&path).expect("open");
    let stream = order_stream();
    store
        .append(&stream, vec![record("OrderPlaced")], ExpectedVersion::Any)
        .await
        .expect("append");

    tamper(&path, "UPDATE events SET payload = '{not json'");

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
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("events.db");
        let store = SqliteEventStore::open(&path).expect("open");
        store
            .append(&order_stream(), vec![record("A")], ExpectedVersion::Any)
            .await
            .expect("append");
        tamper(&path, sql);
        let result = store.read_all(None, FeedReadOptions::default()).await;
        assert_corrupt(&result.expect_err(column), column);
    }
}

#[tokio::test]
async fn stored_event_type_missing_from_the_registry_is_reported() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    let stream = order_stream();
    {
        // Written by an older deployment that knew more event types.
        let store = SqliteEventStore::open(&path).expect("open");
        store
            .append(&stream, vec![record("LegacyEvent")], ExpectedVersion::Any)
            .await
            .expect("append");
    }
    let store = SqliteEventStore::open(&path)
        .expect("reopen")
        .with_registry(registry());

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
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteEventStore::open(dir.path().join("events.db"))
        .expect("open")
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
