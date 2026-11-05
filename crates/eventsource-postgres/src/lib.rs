//! `PostgreSQL` adapters for `eventsource-core`: durable event store, checkpoint store,
//! snapshot store, dead-letter store, lock store, outbox and read model store.
//!
//! The crate implements the core ports and nothing in core knows about it.
//! Every store is opened with a connection URL and a schema name; all tables
//! live in that schema, so stores with the same schema share data and stores
//! with different schemas are fully isolated. Connections are unencrypted
//! (`NoTls`); each store handle owns one connection driven by a task on the
//! current Tokio runtime, so stores must be opened from inside a runtime.

mod db;
pub mod error;
pub mod store;

pub use error::PostgresError;
pub use store::PostgresEventStore;
