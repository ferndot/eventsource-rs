//! In-memory adapters for `eventsource-core`: event store, event bus, checkpoints.
//!
//! Nothing here persists across restarts. Use these for tests, examples, and
//! single-process applications.

mod background;
pub mod bus;
pub mod checkpoint;
pub mod dead_letter;
pub mod lock;
pub mod snapshot;
pub mod store;

pub use bus::InMemoryEventBus;
pub use checkpoint::InMemoryCheckpointStore;
pub use dead_letter::InMemoryDeadLetterStore;
pub use lock::InMemoryLockStore;
pub use snapshot::InMemorySnapshotStore;
pub use store::InMemoryEventStore;
