//! Shared fixtures for adapter test suites: a counter aggregate, stream and lock
//! helpers, and a publisher that records what the outbox relay hands it.
//!
//! These are plain test support. They depend only on `eventsource-core`, never on an
//! adapter, so every adapter's tests can use the same definitions.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventsource_core::error::BackendError;
use eventsource_core::lock::{LockName, LockOwner, LockTtl};
use eventsource_core::outbox::{OutboxMessage, OutboxPublisher};
use eventsource_core::{AggregateId, Category, Decider, DomainEvent, StreamId};
use serde::{Deserialize, Serialize};

/// The category [`Counter`] streams live in.
pub const COUNTER_CATEGORY: &str = "Counter";

/// A counter's only event: `n` was added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CounterEvent {
    /// The counter grew by this amount.
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

/// The running total of a [`Counter`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterState {
    /// The sum of every amount added so far.
    pub sum: u64,
}

/// The error type of [`Counter`], which accepts every command.
#[derive(Debug, thiserror::Error)]
#[error("never rejected")]
pub struct Never;

/// A decider that turns each amount in a command into an [`CounterEvent::Added`]
/// event and sums them into [`CounterState`]. It never rejects a command.
#[derive(Debug)]
pub struct Counter;

impl Decider for Counter {
    /// The amounts to add, one event per amount.
    type Command = Vec<u64>;
    type Event = CounterEvent;
    type State = CounterState;
    type Error = Never;

    const AGGREGATE_TYPE: &'static str = COUNTER_CATEGORY;

    fn initial_state() -> CounterState {
        CounterState { sum: 0 }
    }

    fn decide(_: &CounterState, amounts: Vec<u64>) -> Result<Vec<CounterEvent>, Never> {
        Ok(amounts.into_iter().map(CounterEvent::Added).collect())
    }

    fn evolve(state: &CounterState, event: &CounterEvent) -> CounterState {
        let CounterEvent::Added(n) = event;
        CounterState { sum: state.sum + n }
    }
}

/// The counter stream of the aggregate `id`.
///
/// # Panics
/// Never in practice: the category is a valid constant.
#[must_use]
pub fn stream_for(id: AggregateId) -> StreamId {
    StreamId::new(id, Category::new(COUNTER_CATEGORY).expect("category"))
}

/// A counter stream for a fresh aggregate.
#[must_use]
pub fn stream() -> StreamId {
    stream_for(AggregateId::new())
}

/// A valid lock name.
///
/// # Panics
/// If `n` is not a valid lock name.
#[must_use]
pub fn lock_name(n: &str) -> LockName {
    LockName::new(n).expect("name")
}

/// A valid lock owner.
///
/// # Panics
/// If `o` is not a valid lock owner.
#[must_use]
pub fn lock_owner(o: &str) -> LockOwner {
    LockOwner::new(o).expect("owner")
}

/// A lock lease of `secs` seconds.
///
/// # Panics
/// If `secs` is not a valid lease length.
#[must_use]
pub fn lock_ttl(secs: u64) -> LockTtl {
    LockTtl::new(Duration::from_secs(secs)).expect("ttl")
}

/// An [`OutboxPublisher`] that appends each message payload to a shared list and
/// always succeeds. Keep a clone of the list to read what was published.
#[derive(Debug, Default, Clone)]
pub struct CapturingPublisher(pub Arc<Mutex<Vec<serde_json::Value>>>);

impl OutboxPublisher for CapturingPublisher {
    async fn publish(&self, message: &OutboxMessage) -> Result<(), BackendError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(message.payload.clone());
        Ok(())
    }
}
