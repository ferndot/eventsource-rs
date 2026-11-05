//! The in-memory snapshot store keeps the newest snapshot per stream.

use chrono::Utc;
use eventsource_core::snapshot::{Snapshot, SnapshotStore};
use eventsource_core::{AggregateId, Category, StreamId, Version};
use eventsource_memory::InMemorySnapshotStore;
use serde_json::json;

fn stream() -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new("Order").expect("category"),
    )
}

fn snapshot(stream: &StreamId, version: u64, total: u64) -> Snapshot {
    Snapshot {
        stream: stream.clone(),
        version: Version::new(version),
        schema_version: 1,
        state: json!({ "total": total }),
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn save_then_load_returns_the_latest() {
    let store = InMemorySnapshotStore::new();
    let stream = stream();
    store.save(snapshot(&stream, 5, 50)).await.expect("save 5");
    store
        .save(snapshot(&stream, 10, 100))
        .await
        .expect("save 10");

    let loaded = store.load(&stream).await.expect("load").expect("present");
    assert_eq!(loaded.version, Version::new(10));
    assert_eq!(loaded.state, json!({ "total": 100 }));
    assert_eq!(loaded.schema_version, 1);
    assert_eq!(loaded.stream, stream);
}

#[tokio::test]
async fn load_of_an_unknown_stream_is_none() {
    let store = InMemorySnapshotStore::new();
    store
        .save(snapshot(&stream(), 5, 50))
        .await
        .expect("save other");
    assert!(store.load(&stream()).await.expect("load").is_none());
}

#[tokio::test]
async fn saving_an_older_version_does_not_replace_a_newer_one() {
    let store = InMemorySnapshotStore::new();
    let stream = stream();
    store
        .save(snapshot(&stream, 10, 100))
        .await
        .expect("save 10");
    store
        .save(snapshot(&stream, 5, 50))
        .await
        .expect("stale save is not an error");

    let loaded = store.load(&stream).await.expect("load").expect("present");
    assert_eq!(loaded.version, Version::new(10));
    assert_eq!(loaded.state, json!({ "total": 100 }));
}

#[tokio::test]
async fn snapshots_are_kept_per_stream() {
    let store = InMemorySnapshotStore::new();
    let (a, b) = (stream(), stream());
    store.save(snapshot(&a, 5, 1)).await.expect("save a");
    store.save(snapshot(&b, 7, 2)).await.expect("save b");
    assert_eq!(
        store.load(&a).await.expect("load").expect("a").version,
        Version::new(5)
    );
    assert_eq!(
        store.load(&b).await.expect("load").expect("b").version,
        Version::new(7)
    );
}
