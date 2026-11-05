//! `PostgreSQL` adapters for `eventsource-core`: durable event store, checkpoint store,
//! snapshot store, dead-letter store, lock store, outbox and read model store.
//!
//! The crate implements the core ports and nothing in core knows about it.
//! Every store is opened with a connection URL and a schema name; all tables
//! live in that schema, so stores with the same schema share data and stores
//! with different schemas are fully isolated. Connections are unencrypted
//! (`NoTls`); each store handle owns one connection driven by a task on the
//! current Tokio runtime, so stores must be opened from inside a runtime.

pub mod checkpoint;
mod db;
pub mod dead_letter;
pub mod error;
pub mod lock;
pub mod outbox;
pub mod readmodel;
pub mod snapshot;
pub mod store;

pub use checkpoint::PostgresCheckpointStore;
pub use dead_letter::PostgresDeadLetterStore;
pub use error::PostgresError;
pub use lock::PostgresLockStore;
pub use outbox::PostgresOutboxStore;
pub use readmodel::PostgresReadModelStore;
pub use snapshot::PostgresSnapshotStore;
pub use store::PostgresEventStore;
