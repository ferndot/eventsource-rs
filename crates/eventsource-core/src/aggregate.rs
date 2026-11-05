//! A thin, stateful wrapper around a [`Decider`] for the repository.
//!
//! The wrapper holds the current state and the events not yet persisted. All
//! domain logic still lives in the decider; the wrapper only sequences calls.

use crate::decider::Decider;
use crate::event::EventMeta;
use crate::ids::AggregateId;
use crate::version::Version;

/// An event produced by a command, not yet persisted.
///
/// Its metadata is stamped once, when the command executes. Saving reuses the
/// same ids, so a save that fails ambiguously and is retried cannot write the
/// same event twice under two different ids.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingEvent<E> {
    /// Provenance and the event id, fixed at execution time.
    pub meta: EventMeta,
    /// The domain event.
    pub event: E,
}

/// One aggregate instance: its identity, state, and uncommitted events.
pub struct AggregateRoot<D: Decider> {
    id: AggregateId,
    committed: Version,
    state: D::State,
    uncommitted: Vec<PendingEvent<D::Event>>,
}

impl<D: Decider> AggregateRoot<D> {
    /// A new aggregate with no events, in its initial state.
    #[must_use]
    pub fn new(id: AggregateId) -> Self {
        Self {
            id,
            committed: Version::ZERO,
            state: D::initial_state(),
            uncommitted: Vec::new(),
        }
    }

    /// The aggregate's id.
    #[must_use]
    pub const fn id(&self) -> AggregateId {
        self.id
    }

    /// The current state, including uncommitted events.
    #[must_use]
    pub const fn state(&self) -> &D::State {
        &self.state
    }

    /// Version including uncommitted events.
    #[must_use]
    pub fn version(&self) -> Version {
        let pending = u64::try_from(self.uncommitted.len()).unwrap_or(u64::MAX);
        self.committed.advance(pending)
    }

    /// Version as last persisted. This is the expectation for the next append.
    #[must_use]
    pub const fn committed_version(&self) -> Version {
        self.committed
    }

    /// Events produced since the last save, with their stamped metadata.
    #[must_use]
    pub fn uncommitted_events(&self) -> &[PendingEvent<D::Event>] {
        &self.uncommitted
    }

    /// Replays persisted history. The events are treated as already committed.
    ///
    /// Only call this on an aggregate with no pending events. Otherwise the
    /// history would be applied on top of changes that were never saved.
    pub(crate) fn load_from_history(&mut self, events: impl IntoIterator<Item = D::Event>) {
        debug_assert!(
            self.uncommitted.is_empty(),
            "load_from_history called with pending events"
        );
        for event in events {
            self.state = D::evolve(&self.state, &event);
            self.committed = self.committed.next();
        }
    }

    /// Decides a command and, if accepted, applies its events.
    ///
    /// `stamp` is called once per produced event to fix its metadata. On
    /// rejection the aggregate is unchanged and `stamp` is never called.
    ///
    /// Returns the events the command produced.
    ///
    /// # Errors
    /// Returns the decider's error if the command is rejected.
    pub fn execute(
        &mut self,
        command: D::Command,
        stamp: &mut impl FnMut() -> EventMeta,
    ) -> Result<&[PendingEvent<D::Event>], D::Error> {
        let events = D::decide(&self.state, command)?;
        let start = self.uncommitted.len();
        for event in events {
            self.state = D::evolve(&self.state, &event);
            self.uncommitted.push(PendingEvent {
                meta: stamp(),
                event,
            });
        }
        Ok(&self.uncommitted[start..])
    }

    /// Records that every uncommitted event has been persisted.
    pub fn mark_committed(&mut self) {
        self.committed = self.version();
        self.uncommitted.clear();
    }
}
