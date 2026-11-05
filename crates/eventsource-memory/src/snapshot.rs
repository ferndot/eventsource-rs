//! In-memory snapshot store.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use eventsource_core::StreamId;
use eventsource_core::error::SnapshotError;
use eventsource_core::snapshot::{Snapshot, SnapshotStore};

/// A [`SnapshotStore`] that keeps the newest snapshot per stream in process memory.
#[derive(Default)]
pub struct InMemorySnapshotStore {
    snapshots: Mutex<HashMap<StreamId, Snapshot>>,
}

impl InMemorySnapshotStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl SnapshotStore for InMemorySnapshotStore {
    async fn save(&self, snapshot: Snapshot) -> Result<(), SnapshotError> {
        let mut snapshots = self
            .snapshots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(stored) = snapshots.get(&snapshot.stream)
            && stored.version > snapshot.version
        {
            return Ok(());
        }
        snapshots.insert(snapshot.stream.clone(), snapshot);
        Ok(())
    }

    async fn load(&self, stream: &StreamId) -> Result<Option<Snapshot>, SnapshotError> {
        let snapshots = self
            .snapshots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(snapshots.get(stream).cloned())
    }
}
