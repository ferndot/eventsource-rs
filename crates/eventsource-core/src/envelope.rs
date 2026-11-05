//! Read and write value objects that cross the store boundary.

use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};

use crate::event::EventRecord;
use crate::ids::TenantId;
use crate::position::Position;
use crate::stream_id::{Category, StreamId};
use crate::version::Version;

/// A stored event together with where it was stored.
#[derive(Debug, Clone, PartialEq)]
pub struct EventEnvelope {
    /// The event and its provenance.
    pub record: EventRecord,
    /// The stream the event belongs to.
    pub stream: StreamId,
    /// The event's 1-based position within its stream.
    pub stream_version: Version,
    /// The event's position in the store's global feed.
    pub position: Position,
    /// When the store accepted the event. Never decreases within one store.
    pub stored_at: DateTime<Utc>,
}

/// Result of a successful append.
#[derive(Debug, Clone, PartialEq)]
pub struct AppendResult {
    /// The stream that was appended to.
    pub stream: StreamId,
    /// The stream's version after the append.
    pub new_version: Version,
    /// The stored events, in append order.
    pub envelopes: Vec<EventEnvelope>,
}

/// Direction of a read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadDirection {
    /// Oldest first.
    #[default]
    Forward,
    /// Newest first.
    Backward,
}

/// Options for reading one stream.
///
/// `from` and `to` are inclusive stream versions. With `limit`, a forward read
/// returns the first events in range and a backward read returns the last ones.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamReadOptions {
    /// Read order.
    pub direction: ReadDirection,
    /// First version to include; `None` starts at the beginning.
    pub from: Option<Version>,
    /// Last version to include; `None` reads to the end.
    pub to: Option<Version>,
    /// Maximum number of events to return.
    pub limit: Option<NonZeroUsize>,
}

/// Options for reading the global feed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeedReadOptions {
    /// Only events for this tenant.
    pub tenant_id: Option<TenantId>,
    /// Only events for this category.
    pub category: Option<Category>,
    /// Maximum number of events to return.
    pub limit: Option<NonZeroUsize>,
}
