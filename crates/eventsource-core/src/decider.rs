//! The decider: an aggregate's whole behaviour as three pure functions.
//!
//! Nothing here performs I/O or holds state. That makes every rule testable
//! with plain values, and makes replay a fold over history.

use std::error::Error;

use crate::event::DomainEvent;

/// A pure description of one aggregate type.
///
/// - [`Decider::decide`] checks a command against the current state and returns
///   the events it produces, or rejects it.
/// - [`Decider::evolve`] applies one event to a state. It is total: an event is a
///   fact, so it cannot be refused.
pub trait Decider {
    /// The commands this aggregate accepts.
    type Command;
    /// The events this aggregate produces.
    type Event: DomainEvent;
    /// The aggregate's state, derived entirely from events.
    type State;
    /// Why a command can be rejected.
    type Error: Error + Send + Sync + 'static;

    /// Stream category for this aggregate type. Validated when a repository is built.
    const AGGREGATE_TYPE: &'static str;

    /// The state before any event has occurred.
    fn initial_state() -> Self::State;

    /// Decides which events a command produces, given the current state.
    ///
    /// # Errors
    /// Returns `Self::Error` if the command is not allowed in this state.
    fn decide(state: &Self::State, command: Self::Command)
    -> Result<Vec<Self::Event>, Self::Error>;

    /// Returns the state after `event`.
    fn evolve(state: &Self::State, event: &Self::Event) -> Self::State;
}

/// Folds events over the initial state.
///
/// Replaying history is exactly this: the state is a pure function of the events.
pub fn fold<'a, D, I>(events: I) -> D::State
where
    D: Decider,
    D::Event: 'a,
    I: IntoIterator<Item = &'a D::Event>,
{
    events
        .into_iter()
        .fold(D::initial_state(), |state, event| D::evolve(&state, event))
}

#[cfg(test)]
pub(crate) mod test_support {
    use serde::{Deserialize, Serialize};

    use super::Decider;
    use crate::event::DomainEvent;

    /// A tiny counter aggregate shared by unit tests.
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

    pub(crate) enum CounterCommand {
        Add(Vec<u32>),
        Fail,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("rejected")]
    pub(crate) struct Rejected;

    impl Decider for Counter {
        type Command = CounterCommand;
        type Event = CounterEvent;
        type State = u32;
        type Error = Rejected;
        const AGGREGATE_TYPE: &'static str = "Counter";

        fn initial_state() -> u32 {
            0
        }

        fn decide(_: &u32, command: CounterCommand) -> Result<Vec<CounterEvent>, Rejected> {
            match command {
                CounterCommand::Add(ns) => Ok(ns.into_iter().map(CounterEvent::Added).collect()),
                CounterCommand::Fail => Err(Rejected),
            }
        }

        fn evolve(state: &u32, event: &CounterEvent) -> u32 {
            let CounterEvent::Added(n) = event;
            state + n
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{Counter, CounterCommand, CounterEvent};
    use super::*;

    #[test]
    fn fold_of_no_events_is_the_initial_state() {
        assert_eq!(fold::<Counter, _>(&[]), 0);
    }

    #[test]
    fn fold_applies_events_in_order() {
        let events = [
            CounterEvent::Added(1),
            CounterEvent::Added(2),
            CounterEvent::Added(4),
        ];
        assert_eq!(fold::<Counter, _>(&events), 7);
    }

    #[test]
    fn decide_is_pure_and_can_reject() {
        let events = Counter::decide(&5, CounterCommand::Add(vec![1, 2])).expect("accepted");
        assert_eq!(events, vec![CounterEvent::Added(1), CounterEvent::Added(2)]);
        assert!(Counter::decide(&5, CounterCommand::Fail).is_err());
    }
}
