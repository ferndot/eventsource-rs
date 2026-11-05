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
