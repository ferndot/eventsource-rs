//! Checkpoints persist in the schema.

mod common;

use eventsource_core::checkpoint::{Checkpoint, CheckpointStore};
use eventsource_core::error::CheckpointError;
use eventsource_core::{EventId, Position};
use eventsource_postgres::PostgresCheckpointStore;

fn checkpoint(token: u64, processed: u64) -> Checkpoint {
    Checkpoint {
        position: Some(Position::new("store-a", token)),
        last_event_id: Some(EventId::new()),
        events_processed: processed,
    }
}

async fn open(schema: &str) -> PostgresCheckpointStore {
    PostgresCheckpointStore::open(&common::database_url(), schema)
        .await
        .expect("open")
}

#[tokio::test]
async fn checkpoints_survive_reopen() {
    let schema = common::fresh_schema();
    let saved = checkpoint(7, 7);
    {
        let store = open(&schema).await;
        assert_eq!(store.load("proj").await.expect("load"), None);
        store.save("proj", saved.clone()).await.expect("save");
        store
            .save("other", checkpoint(2, 2))
            .await
            .expect("save other");
    }
    let reopened = open(&schema).await;
    assert_eq!(reopened.load("proj").await.expect("load"), Some(saved));
    assert_eq!(
        reopened
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
    let store = open(&common::fresh_schema()).await;
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
    let store = open(&common::fresh_schema()).await;
    store.save("proj", checkpoint(5, 5)).await.expect("first");
    let newer = checkpoint(9, 9);
    store.save("proj", newer.clone()).await.expect("forward");
    assert_eq!(store.load("proj").await.expect("load"), Some(newer));

    let result = store.save("proj", checkpoint(3, 3)).await;
    assert!(matches!(
        result,
        Err(CheckpointError::Regressed {
            stored: 9,
            attempted: 3,
            ..
        })
    ));
    store
        .save("proj", checkpoint(9, 9))
        .await
        .expect("same count is allowed");
}
