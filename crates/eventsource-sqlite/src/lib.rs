//! `SQLite` adapters for `eventsource-core`: durable event store, checkpoint store, snapshot store, dead-letter store and outbox.
//!
//! The crate implements the core ports and nothing in core knows about it.
//! `SQLite` is bundled, so no system library is required.

pub mod checkpoint;
mod db;
pub mod dead_letter;
pub mod error;
pub mod lock;
pub mod outbox;
pub mod snapshot;
pub mod store;

pub use checkpoint::SqliteCheckpointStore;
pub use dead_letter::SqliteDeadLetterStore;
pub use error::SqliteError;
pub use lock::SqliteLockStore;
pub use outbox::SqliteOutboxStore;
pub use snapshot::SqliteSnapshotStore;
pub use store::SqliteEventStore;
