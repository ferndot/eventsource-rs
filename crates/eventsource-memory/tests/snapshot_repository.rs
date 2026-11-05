//! Repository snapshot path over in-memory adapters.
//!
//! The stream is always the source of truth. These tests plant snapshots whose
//! state differs from what the events say, or events that cannot be decoded,
//! to prove which of the two the repository actually used.

use std::sync::Arc;

use chrono::Utc;
use eventsource_core::event::{EventMeta, EventRecord};
use eventsource_core::snapshot::{Snapshot, SnapshotPolicy, SnapshotStore};
use eventsource_core::store::AggregateStore;
use eventsource_core::{
    AggregateId, AggregateRepository, Category, CommandContext, CorrelationId, Decider,
    DomainEvent, ExpectedVersion, StreamId, Version,
};
use eventsource_memory::{InMemoryEventStore, InMemorySnapshotStore};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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

enum CounterCommand {
    Add(Vec<u64>),
}

#[derive(Debug, thiserror::Error)]
#[error("never rejected")]
struct Never;

struct Counter;

impl Decider for Counter {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type State = CounterState;
    type Error = Never;

    const AGGREGATE_TYPE: &'static str = "Counter";

    fn initial_state() -> CounterState {
        CounterState { sum: 0 }
    }

    fn decide(_: &CounterState, command: CounterCommand) -> Result<Vec<CounterEvent>, Never> {
        let CounterCommand::Add(amounts) = command;
        Ok(amounts.into_iter().map(CounterEvent::Added).collect())
    }

    fn evolve(state: &CounterState, event: &CounterEvent) -> CounterState {
        let CounterEvent::Added(n) = event;
        CounterState { sum: state.sum + n }
    }
}

type Events = Arc<InMemoryEventStore>;
type Snapshots = Arc<InMemorySnapshotStore>;

fn stream(id: AggregateId) -> StreamId {
    StreamId::new(id, Category::new("Counter").expect("category"))
}

fn meta() -> EventMeta {
    EventMeta::new(Utc::now(), CorrelationId::new())
}

/// Appends `Added(n)` events to the store directly, bypassing the repository.
async fn append_good(events: &Events, id: AggregateId, amounts: &[u64], expected: u64) {
    let records = amounts
        .iter()
        .map(|n| EventRecord::from_event(&CounterEvent::Added(*n), meta()).expect("encode"))
        .collect();
    events
        .append(
            &stream(id),
            records,
            ExpectedVersion::Exact(Version::new(expected)),
        )
        .await
        .expect("append");
}

/// Appends `count` events that carry the right type but an undecodable payload.
async fn append_poisoned(events: &Events, id: AggregateId, count: u64) {
    let records = (0..count)
        .map(|_| EventRecord::new(meta(), "Added", json!("not a number")))
        .collect();
    events
        .append(&stream(id), records, ExpectedVersion::NoStream)
        .await
        .expect("append poisoned");
}

fn snapshot(id: AggregateId, version: u64, schema_version: u32, state: Value) -> Snapshot {
    Snapshot {
        stream: stream(id),
        version: Version::new(version),
        schema_version,
        state,
        created_at: Utc::now(),
    }
}

fn plain_repo(events: &Events) -> AggregateRepository<Counter, Events> {
    AggregateRepository::new(Arc::clone(events)).expect("repository")
}

fn snapshot_repo(
    events: &Events,
    snapshots: &Snapshots,
    every: u64,
) -> AggregateRepository<Counter, Events, Snapshots> {
    AggregateRepository::<Counter, Events>::new(Arc::clone(events))
        .expect("repository")
        .with_snapshots(
            Arc::clone(snapshots),
            SnapshotPolicy::every_n_events(every).expect("policy"),
        )
}

#[tokio::test]
async fn load_uses_the_snapshot_and_replays_only_later_events() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let snapshots: Snapshots = Arc::new(InMemorySnapshotStore::new());
    let id = AggregateId::new();
    // Events 1..=3 cannot be decoded; events 4 and 5 are good.
    append_poisoned(&events, id, 3).await;
    append_good(&events, id, &[4, 5], 3).await;

    // Without a snapshot the poisoned history is unreadable, so replay really would touch it.
    assert!(plain_repo(&events).get(id).await.is_err());

    // The snapshot's state is deliberately not what the events would give.
    snapshots
        .save(snapshot(id, 3, 1, json!({ "sum": 1000 })))
        .await
        .expect("save snapshot");

    let aggregate = snapshot_repo(&events, &snapshots, 100)
        .load(id)
        .await
        .expect("loads from snapshot without decoding events 1..=3");
    assert_eq!(aggregate.state(), &CounterState { sum: 1009 });
    assert_eq!(aggregate.version(), Version::new(5));
    assert_eq!(aggregate.committed_version(), Version::new(5));
}
