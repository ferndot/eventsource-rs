# eventsource-rs

Event sourcing for Rust, ported from eventsource-py with idiomatic Rust patterns: pure deciders, generic ports with native `async fn`, and no runtime hidden behind the domain.

State is never stored directly. Each aggregate is a stream of immutable events, and its current state is whatever you get by folding those events over an initial state. Commands are checked against that state by a pure function, and the resulting events are appended to the store with optimistic concurrency.

> **Status:** early (`0.1.0`). The API is still moving. Storage backends beyond in-memory are not implemented yet.

## Features

- **Pure domain.** A `Decider` is three functions: `decide`, `evolve`, and `initial_state`. No I/O, so rules are tested with plain values.
- **Optimistic concurrency.** Appends check `ExpectedVersion`, so a stale writer gets an error instead of silently overwriting. `execute_with_retry` reloads and tries again when another writer wins.
- **Idempotent saves.** Event ids are assigned once when a command executes and are reused on save, so retrying a save does not duplicate events.
- **Validated history.** Loads check stream versions, event types, and schema versions, and report corruption as errors.
- **Global feed and positions.** Every event gets a store-local position, so consumers can read the feed in append order and resume from where they stopped.
- **Subscriptions.** A `SubscriptionRunner` delivers feed pages to handlers in order, checkpoints progress, retries transient failures, and isolates panicking handlers so one bad handler does not stop the others.
- **Pluggable adapters.** Ports live in `eventsource-core`. Adapters implement them, and `eventsource-memory` ships an in-process store, bus, and checkpoint store.
- **Conformance suite.** `eventsource-testing` checks any store implementation against the same behavioral contract.

## Crates

| Crate | Purpose |
| --- | --- |
| [`eventsource-core`](crates/eventsource-core) | Domain types and ports: events, streams, positions, `Decider`, `AggregateRoot`, `AggregateRepository`, bus, checkpoints, subscriptions. Depends on no storage. |
| [`eventsource-memory`](crates/eventsource-memory) | In-memory `InMemoryEventStore`, `InMemoryEventBus`, and `InMemoryCheckpointStore`. For tests, examples, and single-process apps. Nothing persists across restarts. |
| [`eventsource-testing`](crates/eventsource-testing) | Conformance suites that any store backend can run through `run_all`. |

## Quick start

Add the core and memory crates to your `Cargo.toml`:

```toml
[dependencies]
eventsource-core = { git = "https://github.com/ferndot/eventsource-rs" }
eventsource-memory = { git = "https://github.com/ferndot/eventsource-rs" }
```

A counter aggregate, end to end:

```rust
use std::sync::Arc;

use eventsource_core::{
    AggregateId, AggregateRepository, CommandContext, Decider, DomainEvent, EventRegistry,
};
use eventsource_memory::InMemoryEventStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CounterEvent {
    Incremented { by: u64 },
}

impl DomainEvent for CounterEvent {
    fn event_types() -> &'static [&'static str] {
        &["Incremented"]
    }

    fn event_type(&self) -> &'static str {
        match self {
            Self::Incremented { .. } => "Incremented",
        }
    }
}

#[derive(Debug, Error)]
enum CounterError {
    #[error("amount must be positive")]
    ZeroAmount,
}

enum CounterCommand {
    Add(u64),
}

struct Counter;

impl Decider for Counter {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type State = u64;
    type Error = CounterError;

    const AGGREGATE_TYPE: &'static str = "Counter";

    fn initial_state() -> u64 {
        0
    }

    fn decide(_state: &u64, command: CounterCommand) -> Result<Vec<CounterEvent>, CounterError> {
        match command {
            CounterCommand::Add(0) => Err(CounterError::ZeroAmount),
            CounterCommand::Add(by) => Ok(vec![CounterEvent::Incremented { by }]),
        }
    }

    fn evolve(state: &u64, event: &CounterEvent) -> u64 {
        match event {
            CounterEvent::Incremented { by } => state + by,
        }
    }
}

#[tokio::main]
async fn main() {
    // Register every event type the store may need to decode.
    let mut registry = EventRegistry::new();
    registry.register::<CounterEvent>().expect("register");
    let store = InMemoryEventStore::new().with_registry(Arc::new(registry));
    let repo = AggregateRepository::<Counter, _>::new(store).expect("valid category");

    let id = AggregateId::new();
    let ctx = CommandContext::new();
    repo.execute(id, CounterCommand::Add(2), &ctx).await.expect("add 2");
    repo.execute(id, CounterCommand::Add(3), &ctx).await.expect("add 3");

    let counter = repo.get(id).await.expect("load").expect("exists");
    assert_eq!(*counter.state(), 5);

    // The decider rejects this, so nothing is appended.
    assert!(repo.execute(id, CounterCommand::Add(0), &ctx).await.is_err());
}
```

Run the same example from a checkout with:

```sh
cargo run -p eventsource-memory --example quickstart
```

## Concepts

**Decider.** `decide(state, command)` returns the events a command produces or rejects it. `evolve(state, event)` applies a fact to the state and cannot fail, because an event that happened cannot be refused.

**Aggregate root.** `AggregateRoot<D>` holds an aggregate's state, its committed version, and any uncommitted events. `AggregateRepository<D, S>` loads an aggregate from a store, runs a command with a `CommandContext`, and saves the new events.

**Command context.** `CommandContext` carries correlation id, causation id, tenant, and actor metadata. It is passed next to each command rather than stored on the aggregate, so the domain stays free of infrastructure concerns.

**Streams and versions.** Each aggregate has a stream id derived from its category and id. Versions start at 1 and increase by one per event. Writers pass an `ExpectedVersion` (`Any`, `NoStream`, `StreamExists`, or `Exact(v)`) to say what they believe the stream looks like.

**Global feed.** Every appended event also gets a `Position` in a store-wide, append-ordered feed. Read it with `read_all`, passing the last position you processed to resume.

**Subscriptions.** A `SubscriptionRunner` reads the feed in batches, hands each event to its handlers, and stores a `Checkpoint` only after a page succeeds. A restart resumes from the checkpoint, so handlers see events again only if the page they were in did not finish. Configure where to start (`StartFrom`), page size, and a `RetryPolicy`.

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
```

The workspace sets `unsafe_code = "forbid"` and enables clippy's pedantic lints.

## Roadmap

Not yet implemented:

- Snapshots
- Read-model categories
- Dead-letter queue for failing handlers
- Persistent backends (Postgres, SQLite, Redis, Kafka)
- Schema migrations
- Multi-tenant context helpers

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
