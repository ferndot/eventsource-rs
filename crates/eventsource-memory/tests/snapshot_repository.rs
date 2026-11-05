//! Repository snapshot path over in-memory adapters.
//!
//! The stream is always the source of truth. These tests plant snapshots whose
//! state differs from what the events say, or events that cannot be decoded,
//! to prove which of the two the repository actually used.

use std::sync::Arc;

use chrono::Utc;
use eventsource_core::error::SnapshotError;
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

async fn add(repo: &AggregateRepository<Counter, Events, Snapshots>, id: AggregateId, n: &[u64]) {
    repo.execute(id, CounterCommand::Add(n.to_vec()), &CommandContext::new())
        .await
        .expect("execute");
}

#[tokio::test]
async fn save_writes_a_snapshot_at_a_policy_boundary_and_not_elsewhere() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let snapshots: Snapshots = Arc::new(InMemorySnapshotStore::new());
    let repo = snapshot_repo(&events, &snapshots, 3);
    let id = AggregateId::new();
    let stored = || async { snapshots.load(&stream(id)).await.expect("load") };

    add(&repo, id, &[1, 1]).await; // version 2: below the first boundary
    assert!(stored().await.is_none(), "no snapshot before a boundary");

    add(&repo, id, &[1]).await; // version 3: exactly on the boundary
    let at_three = stored().await.expect("snapshot at version 3");
    assert_eq!(at_three.version, Version::new(3));
    assert_eq!(at_three.state, json!({ "sum": 3 }));
    assert_eq!(at_three.schema_version, 1);

    add(&repo, id, &[1]).await; // version 4: between boundaries
    assert_eq!(stored().await.expect("kept").version, Version::new(3));

    add(&repo, id, &[1, 1, 1]).await; // 4 -> 7: steps over the boundary at 6
    let at_seven = stored().await.expect("snapshot after stepping over 6");
    assert_eq!(at_seven.version, Version::new(7));
    assert_eq!(at_seven.state, json!({ "sum": 7 }));
}

#[tokio::test]
async fn corrupt_snapshot_payload_falls_back_to_full_replay() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let snapshots: Snapshots = Arc::new(InMemorySnapshotStore::new());
    let id = AggregateId::new();
    append_good(&events, id, &[1, 2, 3, 4, 5], 0).await;
    // A snapshot at the right version and schema, whose state is not a CounterState.
    snapshots
        .save(snapshot(id, 3, 1, json!({ "sum": "banana" })))
        .await
        .expect("save corrupt snapshot");

    let aggregate = snapshot_repo(&events, &snapshots, 100)
        .load(id)
        .await
        .expect("a corrupt snapshot is not fatal");
    assert_eq!(aggregate.state(), &CounterState { sum: 15 });
    assert_eq!(aggregate.version(), Version::new(5));
}

/// A snapshot store whose reads fail the way a damaged row would.
struct UnreadableSnapshots;

impl SnapshotStore for UnreadableSnapshots {
    async fn save(&self, _: Snapshot) -> Result<(), SnapshotError> {
        Ok(())
    }

    async fn load(&self, _: &StreamId) -> Result<Option<Snapshot>, SnapshotError> {
        Err(SnapshotError::Corrupt("bad row".to_owned()))
    }
}

#[tokio::test]
async fn unreadable_snapshot_store_falls_back_to_full_replay() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let id = AggregateId::new();
    append_good(&events, id, &[2, 2, 2], 0).await;

    let repo = AggregateRepository::<Counter, Events>::new(Arc::clone(&events))
        .expect("repository")
        .with_snapshots(
            UnreadableSnapshots,
            SnapshotPolicy::every_n_events(2).expect("policy"),
        );
    let aggregate = repo
        .load(id)
        .await
        .expect("a snapshot read error is not fatal");
    assert_eq!(aggregate.state(), &CounterState { sum: 6 });
}

#[tokio::test]
async fn snapshot_with_another_schema_version_is_ignored() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let snapshots: Snapshots = Arc::new(InMemorySnapshotStore::new());
    let id = AggregateId::new();
    append_good(&events, id, &[1, 2, 3, 4, 5], 0).await;
    // Decodes fine as a CounterState, but was written under schema version 99.
    snapshots
        .save(snapshot(id, 3, 99, json!({ "sum": 1000 })))
        .await
        .expect("save snapshot");

    let aggregate = snapshot_repo(&events, &snapshots, 100)
        .load(id)
        .await
        .expect("load");
    assert_eq!(aggregate.state(), &CounterState { sum: 15 });
}

/// The same aggregate after its state shape changed: state schema version 2.
struct CounterV2;

impl Decider for CounterV2 {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type State = CounterState;
    type Error = Never;

    const AGGREGATE_TYPE: &'static str = "Counter";

    fn state_schema_version() -> u32 {
        2
    }

    fn initial_state() -> CounterState {
        Counter::initial_state()
    }

    fn decide(state: &CounterState, command: CounterCommand) -> Result<Vec<CounterEvent>, Never> {
        Counter::decide(state, command)
    }

    fn evolve(state: &CounterState, event: &CounterEvent) -> CounterState {
        Counter::evolve(state, event)
    }
}

#[tokio::test]
async fn snapshots_carry_the_deciders_state_schema_version() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let snapshots: Snapshots = Arc::new(InMemorySnapshotStore::new());
    let id = AggregateId::new();
    let v2 = AggregateRepository::<CounterV2, Events>::new(Arc::clone(&events))
        .expect("repository")
        .with_snapshots(
            Arc::clone(&snapshots),
            SnapshotPolicy::every_n_events(2).expect("policy"),
        );
    v2.execute(id, CounterCommand::Add(vec![1, 1]), &CommandContext::new())
        .await
        .expect("execute");

    let written = snapshots
        .load(&stream(id))
        .await
        .expect("load")
        .expect("snapshot");
    assert_eq!(written.schema_version, 2);

    // A v1 reader must not trust it; overwrite state to prove the snapshot is not used.
    snapshots
        .save(snapshot(id, 2, 2, json!({ "sum": 500 })))
        .await
        .expect("save");
    let aggregate = snapshot_repo(&events, &snapshots, 100)
        .load(id)
        .await
        .expect("load");
    assert_eq!(aggregate.state(), &CounterState { sum: 2 });
}

#[tokio::test]
async fn snapshot_ahead_of_the_stream_is_ignored() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let snapshots: Snapshots = Arc::new(InMemorySnapshotStore::new());
    let id = AggregateId::new();
    append_good(&events, id, &[1, 2, 3], 0).await;
    // The stream has 3 events; a snapshot claiming version 10 cannot be right.
    snapshots
        .save(snapshot(id, 10, 1, json!({ "sum": 1000 })))
        .await
        .expect("save snapshot");

    let aggregate = snapshot_repo(&events, &snapshots, 100)
        .load(id)
        .await
        .expect("load");
    assert_eq!(aggregate.state(), &CounterState { sum: 6 });
    assert_eq!(aggregate.version(), Version::new(3));
}

#[tokio::test]
async fn snapshot_at_version_zero_is_ignored() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let snapshots: Snapshots = Arc::new(InMemorySnapshotStore::new());
    let id = AggregateId::new();
    append_good(&events, id, &[1, 2], 0).await;
    // Version zero means "no events folded in", so its state would be applied on top of a full replay.
    snapshots
        .save(snapshot(id, 0, 1, json!({ "sum": 1000 })))
        .await
        .expect("save snapshot");

    let aggregate = snapshot_repo(&events, &snapshots, 100)
        .load(id)
        .await
        .expect("load");
    assert_eq!(aggregate.state(), &CounterState { sum: 3 });
}

/// A snapshot store that is readable but refuses every write.
struct UnwritableSnapshots;

impl SnapshotStore for UnwritableSnapshots {
    async fn save(&self, _: Snapshot) -> Result<(), SnapshotError> {
        Err(SnapshotError::Backend("disk full".into()))
    }

    async fn load(&self, _: &StreamId) -> Result<Option<Snapshot>, SnapshotError> {
        Ok(None)
    }
}

#[tokio::test]
async fn failing_to_write_a_snapshot_does_not_fail_the_save() {
    let events: Events = Arc::new(InMemoryEventStore::new());
    let id = AggregateId::new();
    let repo = AggregateRepository::<Counter, Events>::new(Arc::clone(&events))
        .expect("repository")
        .with_snapshots(
            UnwritableSnapshots,
            SnapshotPolicy::every_n_events(2).expect("policy"),
        );

    let saved = repo
        .execute(id, CounterCommand::Add(vec![1, 1]), &CommandContext::new())
        .await
        .expect("the events are committed, so the save succeeds");
    assert!(saved.is_some());
    assert_eq!(
        repo.stored_version(id).await.expect("version"),
        Version::new(2)
    );
}
