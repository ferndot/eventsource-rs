//! In-memory dead-letter store.

use std::sync::{Mutex, PoisonError};

use eventsource_core::dead_letter::{DeadLetterEntry, DeadLetterId, DeadLetterStore, ParkedEvent};
use eventsource_core::error::DeadLetterError;

/// A [`DeadLetterStore`] that keeps parked events in process memory.
#[derive(Default)]
pub struct InMemoryDeadLetterStore {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    parked: Vec<ParkedEvent>,
}

impl InMemoryDeadLetterStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DeadLetterStore for InMemoryDeadLetterStore {
    async fn park(&self, entry: DeadLetterEntry) -> Result<DeadLetterId, DeadLetterError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let existing = state.parked.iter_mut().find(|p| {
            p.entry.subscription == entry.subscription
                && p.entry.envelope.record.meta.event_id == entry.envelope.record.meta.event_id
        });
        if let Some(existing) = existing {
            existing.entry = entry;
            return Ok(existing.id);
        }
        state.next_id += 1;
        let id = DeadLetterId::new(state.next_id);
        state.parked.push(ParkedEvent { id, entry });
        Ok(id)
    }

    async fn list(&self, subscription: &str) -> Result<Vec<ParkedEvent>, DeadLetterError> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(state
            .parked
            .iter()
            .filter(|p| p.entry.subscription == subscription)
            .cloned()
            .collect())
    }
}
