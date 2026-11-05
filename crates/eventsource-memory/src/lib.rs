#![allow(clippy::unused_async_trait_impl)] // Trait methods return impl Future; async fn is the natural impl form.
//! In-memory adapters for `eventsource-core`: event store, event bus, checkpoints.
//!
//! Nothing here persists across restarts. Use these for tests, examples, and
//! single-process applications.

mod background;
pub mod bus;
pub mod checkpoint;
pub mod store;

pub use bus::InMemoryEventBus;
pub use checkpoint::InMemoryCheckpointStore;
pub use store::InMemoryEventStore;
