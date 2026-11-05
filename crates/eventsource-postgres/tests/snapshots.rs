//! Snapshots persist in the schema and obey the same rules as in memory.

mod common;

use std::sync::Arc;

use chrono::Utc;
use eventsource_core::error::SnapshotError;
use eventsource_core::snapshot::{Snapshot, SnapshotPolicy, SnapshotStore};
use eventsource_core::{
    AggregateId, AggregateRepository, Category, CommandContext, Decider, DomainEvent, StreamId,
    Version,
};
use eventsource_postgres::{PostgresEventStore, PostgresSnapshotStore};
use serde::{Deserialize, Serialize};
use serde_json::json;

fn stream() -> StreamId {
    StreamId::new(
        AggregateId::new(),
        Category::new("Counter").expect("category"),
    )
}

fn snapshot(stream: &StreamId, version: u64, sum: u64) -> Snapshot {
    Snapshot {
        stream: stream.clone(),
        version: Version::new(version),
        schema_version: 3,
        state: json!({ "sum": sum }),
        created_at: Utc::now(),
    }
}

async fn open(schema: &str) -> PostgresSnapshotStore {
    PostgresSnapshotStore::open(&common::database_url(), schema)
        .await
        .expect("open")
}

#[tokio::test]
async fn snapshots_survive_reopen() {
    let schema = common::fresh_schema();
    let (kept, other) = (stream(), stream());
    let saved = snapshot(&kept, 10, 100);
    {
        let store = open(&schema).await;
        assert_eq!(store.load(&kept).await.expect("load"), None);
        store.save(snapshot(&kept, 5, 50)).await.expect("save 5");
        store.save(saved.clone()).await.expect("save 10");
        store
            .save(snapshot(&other, 2, 7))
            .await
            .expect("save other");
    }
    let reopened = open(&schema).await;
    assert_eq!(reopened.load(&kept).await.expect("load"), Some(saved));
    assert_eq!(
        reopened
            .load(&other)
            .await
            .expect("load")
            .map(|s| s.version),
        Some(Version::new(2)),
        "snapshots are keyed by stream"
    );
    assert_eq!(reopened.load(&stream()).await.expect("load"), None);
}

#[tokio::test]
async fn saving_an_older_version_does_not_replace_a_newer_one() {
    let store = open(&common::fresh_schema()).await;
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
    assert_eq!(loaded.state, json!({ "sum": 100 }));
}

#[tokio::test]
async fn an_unreadable_row_is_reported_as_corrupt() {
    let schema = common::fresh_schema();
    let store = open(&schema).await;
    let stream = stream();
    store.save(snapshot(&stream, 4, 4)).await.expect("save");
    common::tamper(&schema, "UPDATE snapshots SET state = '{not json'").await;

    let result = store.load(&stream).await;
    assert!(
        matches!(result, Err(SnapshotError::Corrupt(_))),
        "{result:?}"
    );
}

// ---- Through the repository, with events and snapshots in one file. ----

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum CounterEvent {
    Added(u64),
}

impl DomainEvent for CounterEvent {
    fn event_types() -> &'static [&'static str] {
        &["Added"]
    }

    fn event_type(&self) -> &'static str {
        "Added"
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CounterState {
    sum: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("never rejected")]
struct Never;

struct Counter;

impl Decider for Counter {
    type Command = Vec<u64>;
    type Event = CounterEvent;
    type State = CounterState;
    type Error = Never;

    const AGGREGATE_TYPE: &'static str = "Counter";

    fn state_schema_version() -> u32 {
        3
    }

    fn initial_state() -> CounterState {
        CounterState { sum: 0 }
    }

    fn decide(_: &CounterState, command: Vec<u64>) -> Result<Vec<CounterEvent>, Never> {
        Ok(command.into_iter().map(CounterEvent::Added).collect())
    }

    fn evolve(state: &CounterState, event: &CounterEvent) -> CounterState {
        let CounterEvent::Added(n) = event;
        CounterState { sum: state.sum + n }
    }
}

async fn repo(
    schema: &str,
) -> AggregateRepository<Counter, Arc<PostgresEventStore>, Arc<PostgresSnapshotStore>> {
    let url = common::database_url();
    let events = Arc::new(PostgresEventStore::open(&url, schema).await.expect("event store"));
    let snapshots = Arc::new(PostgresSnapshotStore::open(&url, schema).await.expect("snapshot store"));
    AggregateRepository::<Counter, _>::new(events)
        .expect("repository")
        .with_snapshots(
            snapshots,
            SnapshotPolicy::every_n_events(2).expect("policy"),
        )
}

#[tokio::test]
async fn repository_snapshots_round_trip_through_postgres() {
    let schema = common::fresh_schema();
    let id = AggregateId::new();
    repo(&schema)
        .await
        .execute(id, vec![1, 2, 3], &CommandContext::new())
        .await
        .expect("execute");

    // Reopen: the snapshot written at version 3 (crossing 2) is in the schema.
    let raw = common::raw_client().await;
    raw.batch_execute(&format!("SET search_path TO \"{schema}\""))
        .await
        .expect("search path");
    let version: i64 = raw
        .query_one("SELECT version FROM snapshots", &[])
        .await
        .expect("one snapshot row")
        .get(0);
    assert_eq!(version, 3);

    // Plant a different state at the same version to prove the load uses the snapshot.
    raw.execute("UPDATE snapshots SET state = '{\"sum\":500}'", &[])
        .await
        .expect("plant");
    let loaded = repo(&schema).await.load(id).await.expect("load");
    assert_eq!(loaded.state(), &CounterState { sum: 500 });
    assert_eq!(loaded.version(), Version::new(3));

    // Damage the row: the repository falls back to the events.
    raw.execute("UPDATE snapshots SET state = '{not json'", &[])
        .await
        .expect("damage");
    let loaded = repo(&schema).await.load(id).await.expect("fallback load");
    assert_eq!(loaded.state(), &CounterState { sum: 6 });
}
