//! Checkpoint store behaviour, mirroring the `SQLite` and `PostgreSQL` checkpoint tests.
//!
//! "Survives reopen" has no in-memory equivalent, so it becomes "load after save
//! returns the same value".

use eventsource_core::checkpoint::{Checkpoint, CheckpointStore};
use eventsource_core::error::CheckpointError;
use eventsource_core::{EventId, Position};
use eventsource_memory::InMemoryCheckpointStore;

fn checkpoint(token: u64, processed: u64) -> Checkpoint {
    Checkpoint {
        position: Some(Position::new("store-a", token)),
        last_event_id: Some(EventId::new()),
        events_processed: processed,
    }
}

#[tokio::test]
async fn load_after_save_returns_the_same_value() {
    let store = InMemoryCheckpointStore::new();
    let saved = checkpoint(7, 7);
    assert_eq!(store.load("proj").await.expect("load"), None);
    store.save("proj", saved.clone()).await.expect("save");
    store
        .save("other", checkpoint(2, 2))
        .await
        .expect("save other");
    assert_eq!(store.load("proj").await.expect("load"), Some(saved));
    assert_eq!(
        store
            .load("other")
            .await
            .expect("load")
            .map(|c| c.events_processed),
        Some(2),
        "checkpoints are keyed by name"
    );
}

#[tokio::test]
async fn an_empty_checkpoint_round_trips() {
    let store = InMemoryCheckpointStore::new();
    store
        .save("fresh", Checkpoint::default())
        .await
        .expect("save");
    assert_eq!(
        store.load("fresh").await.expect("load"),
        Some(Checkpoint::default())
    );
}

#[tokio::test]
async fn save_replaces_and_rejects_regression() {
    let store = InMemoryCheckpointStore::new();
    store.save("proj", checkpoint(5, 5)).await.expect("first");
    let newer = checkpoint(9, 9);
    store.save("proj", newer.clone()).await.expect("forward");
    assert_eq!(store.load("proj").await.expect("load"), Some(newer.clone()));

    let result = store.save("proj", checkpoint(3, 3)).await;
    assert!(matches!(
        result,
        Err(CheckpointError::Regressed {
            stored: 9,
            attempted: 3,
            ..
        })
    ));
    assert_eq!(
        store.load("proj").await.expect("load"),
        Some(newer),
        "a rejected save leaves the stored checkpoint alone"
    );
    store
        .save("proj", checkpoint(9, 9))
        .await
        .expect("same count is allowed");
}
