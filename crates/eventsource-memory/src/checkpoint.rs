//! In-memory checkpoint store.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use eventsource_core::checkpoint::{Checkpoint, CheckpointStore};
use eventsource_core::error::CheckpointError;

/// A [`CheckpointStore`] that keeps checkpoints in process memory.
#[derive(Default)]
pub struct InMemoryCheckpointStore {
    checkpoints: Mutex<HashMap<String, Checkpoint>>,
}

impl InMemoryCheckpointStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    async fn load(&self, name: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        let checkpoints = self
            .checkpoints
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(checkpoints.get(name).cloned())
    }

    async fn save(&self, name: &str, checkpoint: Checkpoint) -> Result<(), CheckpointError> {
        let mut checkpoints = self
            .checkpoints
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(stored) = checkpoints.get(name)
            && checkpoint.events_processed < stored.events_processed
        {
            return Err(CheckpointError::Regressed {
                name: name.to_owned(),
                stored: stored.events_processed,
                attempted: checkpoint.events_processed,
            });
        }
        checkpoints.insert(name.to_owned(), checkpoint);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn save_rejects_a_checkpoint_that_moves_backwards() {
        let store = InMemoryCheckpointStore::new();
        let at = |events_processed| Checkpoint {
            events_processed,
            ..Checkpoint::default()
        };
        store.save("proj", at(5)).await.expect("forward");
        let result = store.save("proj", at(3)).await;
        assert!(matches!(
            result,
            Err(CheckpointError::Regressed {
                stored: 5,
                attempted: 3,
                ..
            })
        ));
        store.save("proj", at(5)).await.expect("same is allowed");
    }
}
