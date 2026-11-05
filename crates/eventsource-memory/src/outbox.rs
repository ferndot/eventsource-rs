//! In-memory outbox store.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use eventsource_core::error::OutboxError;
use eventsource_core::outbox::{
    NewOutboxMessage, OutboxId, OutboxMessage, OutboxStatus, OutboxStore,
};

#[derive(Default)]
pub(crate) struct State {
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

impl State {
    pub(crate) fn push(&mut self, message: NewOutboxMessage) -> OutboxId {
        self.next_id += 1;
        let id = OutboxId::new(self.next_id);
        self.messages.push(OutboxMessage {
            id,
            destination: message.destination,
            payload: message.payload,
            stream: message.stream,
            attempts: 0,
            status: OutboxStatus::Pending,
        });
        id
    }
}

impl InMemoryOutboxStore {
    /// The messages, locked. Callers must not hold the guard across an `.await`.
    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl OutboxStore for InMemoryOutboxStore {
    async fn enqueue(&self, message: NewOutboxMessage) -> Result<OutboxId, OutboxError> {
        message.validate()?;
        Ok(self.lock().push(message))
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

    async fn mark_published(&self, id: OutboxId) -> Result<(), OutboxError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(message) = state.messages.iter_mut().find(|m| m.id == id) {
            message.status = OutboxStatus::Published;
        }
        Ok(())
    }

    async fn mark_failed(&self, id: OutboxId, _error: &str) -> Result<(), OutboxError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(message) = state.messages.iter_mut().find(|m| m.id == id) {
            message.attempts += 1;
        }
        Ok(())
    }
}
