//! In-memory outbox store.

use std::sync::{Arc, Mutex, PoisonError};

use eventsource_core::error::OutboxError;
use eventsource_core::outbox::{
    NewOutboxMessage, OutboxId, OutboxMessage, OutboxStatus, OutboxStore,
};

#[derive(Default)]
struct State {
    next_id: u64,
    messages: Vec<OutboxMessage>,
}

/// An [`OutboxStore`] that keeps messages in process memory.
///
/// Clones share the same messages.
#[derive(Clone, Default)]
pub struct InMemoryOutboxStore {
    state: Arc<Mutex<State>>,
}

impl InMemoryOutboxStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl OutboxStore for InMemoryOutboxStore {
    async fn enqueue(&self, message: NewOutboxMessage) -> Result<OutboxId, OutboxError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.next_id += 1;
        let id = OutboxId::new(state.next_id);
        state.messages.push(OutboxMessage {
            id,
            destination: message.destination,
            payload: message.payload,
            stream: message.stream,
            attempts: 0,
            status: OutboxStatus::Pending,
        });
        Ok(id)
    }

    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxMessage>, OutboxError> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(state
            .messages
            .iter()
            .filter(|m| m.status == OutboxStatus::Pending)
            .take(limit)
            .cloned()
            .collect())
    }
}
