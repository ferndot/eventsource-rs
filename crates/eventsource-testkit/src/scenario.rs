//! Given-when-then scenarios over a [`Decider`], with no store, bus or runtime.

use std::fmt::Debug;

use eventsource_core::Decider;

/// Starts a given-when-then test of a [`Decider`].
///
/// Only `decide` and `evolve` run, so scenarios are plain synchronous tests.
///
/// ```ignore
/// DeciderScenario::<Counter>::new()
///     .given([CounterEvent::Added(4)])
///     .when(CounterCommand::Add(2))
///     .then_events([CounterEvent::Added(2)]);
/// ```
pub struct DeciderScenario<D: Decider> {
    state: D::State,
}

impl<D: Decider> Default for DeciderScenario<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: Decider> DeciderScenario<D> {
    /// A scenario at the decider's initial state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: D::initial_state(),
        }
    }

    /// Replays `events` as history, in order.
    #[must_use]
    pub fn given(mut self, events: impl IntoIterator<Item = D::Event>) -> Self {
        for event in events {
            self.state = D::evolve(&self.state, &event);
        }
        self
    }

    /// Runs `command` against the state built by [`given`](Self::given).
    #[must_use]
    pub fn when(self, command: D::Command) -> ScenarioResult<D> {
        ScenarioResult {
            outcome: D::decide(&self.state, command),
        }
    }
}

/// What a command produced. Assert on it with the `then_*` methods.
///
/// Every `then_*` method panics with a message naming the expectation and
/// what actually happened.
pub struct ScenarioResult<D: Decider> {
    outcome: Result<Vec<D::Event>, D::Error>,
}

impl<D: Decider> ScenarioResult<D> {
    /// Asserts the command was accepted and produced exactly `expected`, in order.
    ///
    /// # Panics
    /// If the events differ, or the command was rejected.
    pub fn then_events(&self, expected: impl IntoIterator<Item = D::Event>)
    where
        D::Event: PartialEq,
    {
        let expected: Vec<D::Event> = expected.into_iter().collect();
        match &self.outcome {
            Ok(actual) => assert!(
                *actual == expected,
                "expected events {expected:?}, got {actual:?}"
            ),
            Err(error) => {
                panic!("expected events {expected:?}, but the command was rejected: {error}")
            }
        }
    }

    /// Asserts the command was rejected with `expected`.
    ///
    /// # Panics
    /// If the error differs, or the command was accepted.
    #[allow(clippy::needless_pass_by_value)] // By value reads best at the call site: `.then_error(MyError::X)`.
    pub fn then_error(&self, expected: D::Error)
    where
        D::Error: PartialEq + Debug,
    {
        match &self.outcome {
            Err(actual) => assert!(
                *actual == expected,
                "expected error {expected:?}, got {actual:?}"
            ),
            Ok(events) => {
                panic!("expected error {expected:?}, but the command produced {events:?}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{Counter, CounterCommand, CounterError, CounterEvent};

    #[test]
    fn given_when_then_events_passes() {
        DeciderScenario::<Counter>::new()
            .given([CounterEvent::Added(4), CounterEvent::Added(3)])
            .when(CounterCommand::Add(2))
            .then_events([CounterEvent::Added(2)]);
    }

    #[test]
    #[should_panic(expected = "expected events [Added(9)], got [Added(2)]")]
    fn then_events_mismatch_names_expected_and_actual() {
        DeciderScenario::<Counter>::new()
            .when(CounterCommand::Add(2))
            .then_events([CounterEvent::Added(9)]);
    }

    #[test]
    fn then_error_matches_the_deciders_error() {
        DeciderScenario::<Counter>::new()
            .given([CounterEvent::Added(9)])
            .when(CounterCommand::Add(2))
            .then_error(CounterError::Overflow(10));
    }

    #[test]
    #[should_panic(expected = "expected error ZeroAmount, got Overflow(10)")]
    fn then_error_mismatch_panics() {
        DeciderScenario::<Counter>::new()
            .given([CounterEvent::Added(9)])
            .when(CounterCommand::Add(2))
            .then_error(CounterError::ZeroAmount);
    }

    #[test]
    #[should_panic(expected = "expected error ZeroAmount, but the command produced [Added(2)]")]
    fn then_error_when_command_succeeded_panics() {
        DeciderScenario::<Counter>::new()
            .when(CounterCommand::Add(2))
            .then_error(CounterError::ZeroAmount);
    }

    #[test]
    #[should_panic(
        expected = "expected events [Added(2)], but the command was rejected: amount must be positive"
    )]
    fn then_events_when_command_rejected_panics() {
        DeciderScenario::<Counter>::new()
            .when(CounterCommand::Add(0))
            .then_events([CounterEvent::Added(2)]);
    }
}
