//! Checkpoint port: where a projection or subscriber has read up to.

use std::future::Future;

use crate::error::CheckpointError;
use crate::ids::EventId;
use crate::position::Position;

/// Progress of one named consumer through the global feed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checkpoint {
    /// The last position processed. `None` before the first event.
    pub position: Option<Position>,
    /// The last event processed.
    pub last_event_id: Option<EventId>,
    /// How many events have been processed in total.
    pub events_processed: u64,
}

impl<T: CheckpointStore> CheckpointStore for std::sync::Arc<T> {
    fn load(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Option<Checkpoint>, CheckpointError>> + Send {
        (**self).load(name)
    }

    fn save(
        &self,
        name: &str,
        checkpoint: Checkpoint,
    ) -> impl Future<Output = Result<(), CheckpointError>> + Send {
        (**self).save(name, checkpoint)
    }
}

/// Durable storage for [`Checkpoint`]s, keyed by consumer name.
///
/// Checkpoints must never move backwards: `events_processed` may stay the same
/// or grow, and a save that would lower it is rejected with `Regressed`.
pub trait CheckpointStore: Send + Sync {
    /// The checkpoint for `name`, if one has been saved.
    fn load(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Option<Checkpoint>, CheckpointError>> + Send;

    /// Replaces the checkpoint for `name`.
    fn save(
        &self,
        name: &str,
        checkpoint: Checkpoint,
    ) -> impl Future<Output = Result<(), CheckpointError>> + Send;
}
