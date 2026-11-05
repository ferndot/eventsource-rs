//! A tiny counter aggregate shared by this crate's unit tests.

use eventsource_core::{Decider, DomainEvent};
use serde::{Deserialize, Serialize};

pub(crate) struct Counter;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum CounterEvent {
    Added(u32),
}

impl DomainEvent for CounterEvent {
    fn event_types() -> &'static [&'static str] {
        &["Added"]
    }
    fn event_type(&self) -> &'static str {
        "Added"
    }
}

#[derive(Debug)]
pub(crate) enum CounterCommand {
    Add(u32),
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum CounterError {
    #[error("amount must be positive")]
    ZeroAmount,
    #[error("total would exceed {0}")]
    Overflow(u32),
}

impl Decider for Counter {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type State = u32;
    type Error = CounterError;
    const AGGREGATE_TYPE: &'static str = "Counter";

    fn initial_state() -> u32 {
        0
    }

    fn decide(state: &u32, command: CounterCommand) -> Result<Vec<CounterEvent>, CounterError> {
        let CounterCommand::Add(n) = command;
        if n == 0 {
            Err(CounterError::ZeroAmount)
        } else if state + n > 10 {
            Err(CounterError::Overflow(10))
        } else {
            Ok(vec![CounterEvent::Added(n)])
        }
    }

    fn evolve(state: &u32, event: &CounterEvent) -> u32 {
        let CounterEvent::Added(n) = event;
        state + n
    }
}
