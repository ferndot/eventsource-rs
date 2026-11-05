//! `SQLite` adapters for `eventsource-core`: durable event store and checkpoint store.
//!
//! The crate implements the core ports and nothing in core knows about it.
//! `SQLite` is bundled, so no system library is required.

mod db;
pub mod error;
pub mod store;

pub use error::SqliteError;
pub use store::SqliteEventStore;
