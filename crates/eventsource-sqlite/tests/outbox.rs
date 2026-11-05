//! The outbox persists in the database file and survives a restart.

use std::sync::{Arc, Mutex};

use eventsource_core::error::BackendError;
use eventsource_core::outbox::{
    NewOutboxMessage, OutboxMessage, OutboxPublisher, OutboxStore, relay,
};
use eventsource_sqlite::SqliteOutboxStore;
use serde_json::json;

struct CapturingPublisher(Arc<Mutex<Vec<serde_json::Value>>>);

impl OutboxPublisher for CapturingPublisher {
    async fn publish(&self, message: &OutboxMessage) -> Result<(), BackendError> {
        self.0.lock().expect("lock").push(message.payload.clone());
        Ok(())
    }
}

#[tokio::test]
async fn pending_messages_survive_reopen_and_are_delivered_after_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("outbox.db");
    {
        let outbox = SqliteOutboxStore::open(&path).expect("open");
        for n in 0..3 {
            outbox
                .enqueue(NewOutboxMessage::new("orders", json!({ "n": n })))
                .await
                .expect("enqueue");
        }
        // The process "crashes" here: nothing was published.
    }

    let outbox = SqliteOutboxStore::open(&path).expect("reopen");
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

    let outbox = SqliteOutboxStore::open(&path).expect("reopen again");
    assert!(outbox.fetch_pending(10).await.expect("fetch").is_empty());
}
