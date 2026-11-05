//! Error types, one per port, so callers match on exactly what can go wrong.

use crate::bus::HandlerFailure;
use crate::ids::{AggregateId, EventId};
use crate::stream_id::StreamId;
use crate::version::{ExpectedVersion, Version};

/// Boxed error from a store, bus, or checkpoint backend.
pub type BackendError = Box<dyn std::error::Error + Send + Sync>;

/// Error parsing a stream category or stream id.
#[derive(Debug, thiserror::Error)]
pub enum StreamIdError {
    /// The category is empty or contains disallowed characters.
    #[error("invalid stream category {0:?}: must be non-empty and match [A-Za-z0-9_.-]+")]
    InvalidCategory(String),
    /// The text has no `:` separator.
    #[error("malformed stream id {0:?}: expected \"<uuid>:<category>\"")]
    Malformed(String),
    /// The id part is not a UUID.
    #[error("invalid aggregate id: {0}")]
    Uuid(#[from] uuid::Error),
}

/// Error comparing positions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PositionError {
    /// The positions come from different stores and cannot be ordered.
    #[error("cannot order positions from different stores: {left:?} and {right:?}")]
    Foreign {
        /// Store of the left-hand position.
        left: String,
        /// Store of the right-hand position.
        right: String,
    },
}

/// Error from event type registration.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// A wire name is already owned by a different event type.
    #[error("event type {name:?} is already registered to a different type")]
    DuplicateEventType {
        /// The conflicting wire name.
        name: String,
    },
}

/// Error from an event store.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The stream's version did not match the expectation.
    #[error("concurrency conflict on {stream}: expected {expected:?}, stream is at {actual:?}")]
    ConcurrencyConflict {
        /// The stream that was written to.
        stream: StreamId,
        /// What the writer expected.
        expected: ExpectedVersion,
        /// What the stream actually contains.
        actual: Version,
    },
    /// An event with this id is already stored.
    #[error("event {0} is already stored")]
    DuplicateEvent(EventId),
    /// An append contained no events.
    #[error("cannot append an empty batch")]
    EmptyAppend,
    /// The event type is not in the store's registry.
    #[error("event type {0:?} is not registered")]
    UnregisteredEventType(String),
    /// A feed read was given a position from another store.
    #[error(transparent)]
    Position(#[from] PositionError),
    /// The backend failed. The cause is available through `source()`.
    #[error("store backend failed")]
    Backend(#[source] BackendError),
}

/// Error from an event bus.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    /// One or more handlers failed. Every handler still ran before this was returned.
    #[error("{} handler(s) failed", .0.len())]
    HandlerFailures(Vec<HandlerFailure>),
}

/// Error from a checkpoint store.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// The backend failed. The cause is available through `source()`.
    #[error("checkpoint backend failed")]
    Backend(#[source] BackendError),
    /// The new checkpoint would move backwards, which would replay events.
    #[error("checkpoint {name:?} would move backwards from {stored} to {attempted} events")]
    Regressed {
        /// The consumer whose checkpoint was rejected.
        name: String,
        /// Events processed according to the saved checkpoint.
        stored: u64,
        /// Events processed according to the rejected checkpoint.
        attempted: u64,
    },
}

/// Error turning a stored record back into a domain event.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The payload does not match the event type.
    #[error("payload does not match the event type: {0}")]
    Payload(#[from] serde_json::Error),
    /// The stored event type does not match the variant the payload decodes to.
    #[error("stored event type {stored:?} does not match decoded type {decoded:?}")]
    EventTypeMismatch {
        /// Type recorded in the store.
        stored: String,
        /// Type of the decoded payload.
        decoded: String,
    },
    /// The record was written with a schema version this code does not read.
    #[error("schema version {found} is not supported (expected {supported})")]
    UnsupportedSchemaVersion {
        /// Version recorded in the store.
        found: u32,
        /// Version this code reads.
        supported: u32,
    },
}

/// Error building a [`crate::snapshot::SnapshotPolicy`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SnapshotPolicyError {
    /// A snapshot interval of zero events is meaningless.
    #[error("snapshot interval must be at least one event")]
    ZeroInterval,
}

/// Error from an aggregate repository.
///
/// `E` is the decider's own error type, so domain rejections stay typed.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError<E: std::error::Error + 'static> {
    /// The decider rejected the command.
    #[error("command rejected")]
    Domain(#[source] E),
    /// The store failed or detected a conflict.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A stored record could not be decoded into the event type.
    #[error("failed to decode stored event: {0}")]
    Decode(#[from] DecodeError),
    /// An event could not be serialized for storage.
    #[error("failed to encode event")]
    Encode(#[source] serde_json::Error),
    /// The stored history for a stream is inconsistent.
    #[error("stream {stream} is corrupt: {detail}")]
    CorruptStream {
        /// The stream that failed the check.
        stream: StreamId,
        /// What was wrong.
        detail: String,
    },
    /// No events exist for the aggregate.
    #[error("aggregate {id} not found")]
    NotFound {
        /// The missing aggregate.
        id: AggregateId,
    },
}

/// Error from a subscription runner. Each variant leaves the checkpoint unchanged for the failed page.
#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    /// A page could not be delivered after every allowed attempt. Restarting redelivers it.
    #[error("subscription {name:?}: handlers failed after {attempts} attempts")]
    Handler {
        /// The subscription that gave up.
        name: String,
        /// How many attempts were made.
        attempts: u32,
        /// The failure from the last attempt.
        #[source]
        last: BusError,
    },
    /// The feed could not be read.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The checkpoint could not be loaded or saved.
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
}
