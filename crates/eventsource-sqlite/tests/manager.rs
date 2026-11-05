//! A subscription manager over SQLite survives a restart: a new manager resumes from the saved checkpoint.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use eventsource_core::bus::EventPublisher;
use eventsource_core::error::BusError;
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::manager::{SubscriptionManager, SubscriptionStatus};
use eventsource_core::store::AggregateStore;
use eventsource_core::{
    AggregateId, Category, CorrelationId, EventEnvelope, EventId, ExpectedVersion, StreamId,
    SubscriptionConfig, SubscriptionRunner,
};
use eventsource_sqlite::{SqliteCheckpointStore, SqliteEventStore};

#[derive(Clone, Default)]
struct Collector {
    ids: Arc<Mutex<Vec<EventId>>>,
}

impl Collector {
    fn ids(&self) -> Vec<EventId> {
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl EventPublisher for Collector {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        let mut ids = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
        ids.extend(envelopes.iter().map(|e| e.record.meta.event_id));
        Ok(())
    }
}

async fn append(store: &SqliteEventStore, count: usize) -> Vec<EventId> {
    let mut ids = Vec::new();
    for n in 0..count {
        let meta = EventMeta::new(chrono::Utc::now(), CorrelationId::new());
        ids.push(meta.event_id);
        let record = EventRecord::new(meta, "Thing", serde_json::json!({ "n": n }));
        let stream = StreamId::new(AggregateId::new(), Category::new("thing").unwrap());
        store
            .append(&stream, vec![record], ExpectedVersion::Any)
            .await
            .expect("append");
    }
    ids
}

async fn eventually(mut done: impl FnMut() -> bool) {
    for _ in 0..500 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within 5s");
}

/// Starts a manager with one subscription `"projection"` over the database in `dir`.
fn start(
    dir: &std::path::Path,
    sink: &Collector,
) -> (SubscriptionManager, tokio::task::JoinHandle<()>) {
    let store = Arc::new(SqliteEventStore::open(dir.join("events.db")).expect("open store"));
    let checkpoints = Arc::new(
        SqliteCheckpointStore::open(dir.join("checkpoints.db")).expect("open checkpoints"),
    );
    let (manager, driver) = SubscriptionManager::new();
    let sink = sink.clone();
    manager
        .register("projection", move || {
            let mut config = SubscriptionConfig::new("projection");
            config.batch_size = NonZeroUsize::new(2).unwrap();
            config.poll_interval = Duration::from_millis(10);
            SubscriptionRunner::new(
                Arc::clone(&store),
                Arc::clone(&checkpoints),
                sink.clone(),
                config,
            )
        })
        .unwrap();
    (manager, tokio::spawn(driver.run()))
}

#[tokio::test]
async fn manager_resumes_from_the_saved_checkpoint_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteEventStore::open(dir.path().join("events.db")).expect("open store");
    let first = append(&store, 5).await;

    let before = Collector::default();
    let (manager, driver) = start(dir.path(), &before);
    eventually(|| before.ids().len() == 5).await;
    assert_eq!(before.ids(), first);
    manager.shutdown().await;
    driver.await.expect("driver ends");
    drop(manager);

    // "Restart": new adapters over the same files, new manager, new sink.
    let second = append(&store, 3).await;
    let after = Collector::default();
    let (manager, driver) = start(dir.path(), &after);
    eventually(|| after.ids().len() == 3).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        after.ids(),
        second,
        "only events after the checkpoint are delivered"
    );

    let health = manager.health("projection").unwrap();
    assert_eq!(health.status, SubscriptionStatus::Running);
    assert_eq!(health.events_processed, 8);
    assert!(health.last_position.is_some());
    manager.shutdown().await;
    driver.await.expect("driver ends");
}
