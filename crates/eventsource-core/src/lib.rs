//! Core event sourcing types and ports.
//!
//! The domain is pure: a [`decider::Decider`] maps `(state, command)` to events
//! and `(state, event)` to the next state, with no I/O. Storage, publishing and
//! checkpointing are expressed as traits here and implemented by adapter crates.

pub mod aggregate;
pub mod bus;
pub mod checkpoint;
pub mod clock;
pub mod coordination;
pub mod dead_letter;
pub mod decider;
pub mod envelope;
pub mod error;
pub mod event;
pub mod ids;
pub mod live;
pub mod lock;
pub mod outbox;
pub mod ownership;
pub mod position;
pub mod projection;
pub mod readmodel;
pub mod registry;
pub mod repository;
pub mod snapshot;
pub mod store;
pub mod stream_id;
pub mod subscription;
pub mod telemetry;
pub mod version;

pub use aggregate::AggregateRoot;
pub use bus::{EventPublisher, Handler, NoPublisher, SubscriptionId};
pub use checkpoint::{Checkpoint, CheckpointStore};
pub use clock::{Clock, SystemClock};
pub use dead_letter::{
    DeadLetterEntry, DeadLetterId, DeadLetterStore, ParkedEvent, RetryParkedError, retry_parked,
};
pub use decider::Decider;
pub use envelope::{
    AppendResult, EventEnvelope, FeedReadOptions, ReadDirection, StreamReadOptions,
};
pub use error::{
    AtomicAppendError, BusError, CheckpointError, DeadLetterError, DecodeError, OutboxError,
    ReadModelError, RepositoryError, StoreError, SubscriptionError,
};
pub use event::{DomainEvent, EventMeta, EventRecord};
pub use ids::{AggregateId, CausationId, CorrelationId, EventId, TenantId};
pub use live::LiveFeed;
pub use lock::{LockError, LockLease, LockName, LockOwner, LockStore, LockTtl, LockValueError};
pub use outbox::{
    NewOutboxMessage, OutboxAppend, OutboxId, OutboxMessage, OutboxPublisher, OutboxStatus,
    OutboxStore, RelayReport, relay,
};
pub use position::Position;
pub use projection::{ProjectionSink, Projector, ReadModelChange, ReadView};
pub use readmodel::ReadModelStore;
pub use registry::EventRegistry;
pub use repository::{AggregateRepository, CommandContext};
pub use store::{AggregateStore, GlobalFeed};
pub use stream_id::{Category, StreamId};
pub use subscription::{RetryPolicy, RunStats, StartFrom, SubscriptionConfig, SubscriptionRunner};
pub use telemetry::{NoopTracer, Tracer};
pub use version::{ExpectedVersion, Version};
