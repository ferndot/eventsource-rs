//! Run with: `cargo run -p eventsource-memory --example quickstart`

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
    repo.execute(id, CounterCommand::Add(2), &ctx)
        .await
        .expect("add 2");
    repo.execute(id, CounterCommand::Add(3), &ctx)
        .await
        .expect("add 3");

    let counter = repo.get(id).await.expect("load").expect("exists");
    assert_eq!(*counter.state(), 5);

    // The decider rejects this, so nothing is appended.
    assert!(
        repo.execute(id, CounterCommand::Add(0), &ctx)
            .await
            .is_err()
    );
}
