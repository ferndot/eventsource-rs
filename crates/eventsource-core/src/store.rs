//! Store ports. Adapters implement these; the repository depends only on them.
//!
//! The traits use native `async fn`-style signatures (`impl Future + Send`), so
//! implementations are monomorphized and never boxed. Consumers take the store
//! as a type parameter, which keeps every call statically dispatched.

use std::future::Future;

use crate::envelope::{AppendResult, EventEnvelope, FeedReadOptions, StreamReadOptions};
use crate::error::StoreError;
use crate::event::EventRecord;
use crate::position::Position;
use crate::stream_id::StreamId;
use crate::version::{ExpectedVersion, Version};

/// Append and read individual streams. This is all an aggregate repository needs.
pub trait AggregateStore: Send + Sync {
    /// Appends `events` to `stream` if the stream matches `expected`.
    ///
    /// The append is atomic: either every event is stored or none is.
    ///
    /// # Errors
    /// [`StoreError::ConcurrencyConflict`] if `expected` is not met,
    /// [`StoreError::DuplicateEvent`] if an event id is already stored,
    /// [`StoreError::EmptyAppend`] for an empty batch.
    fn append(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
    ) -> impl Future<Output = Result<AppendResult, StoreError>> + Send;

    /// Reads events from one stream, in the order given by `options`.
    fn read_stream(
        &self,
        stream: &StreamId,
        options: StreamReadOptions,
    ) -> impl Future<Output = Result<Vec<EventEnvelope>, StoreError>> + Send;

    /// The number of events in `stream`; zero if it does not exist.
    fn stream_version(
        &self,
        stream: &StreamId,
    ) -> impl Future<Output = Result<Version, StoreError>> + Send;
}

/// Read the store-wide feed of every event, in position order.
pub trait GlobalFeed: Send + Sync {
    /// Reads events strictly after `from` (or from the start if `None`).
    ///
    /// Returns a `Vec`, so the caller controls memory with `options.limit`. A
    /// page is the unit of checkpointing, so callers that read the whole feed
    /// should page with `limit` and resume from the last position of each page.
    /// An unbounded read is allowed, but it loads everything at once.
    ///
    /// # Errors
    /// [`StoreError::Position`] if `from` came from a different store.
    fn read_all(
        &self,
        from: Option<&Position>,
        options: FeedReadOptions,
    ) -> impl Future<Output = Result<Vec<EventEnvelope>, StoreError>> + Send;

    /// The position of the newest event, or `None` if the store is empty.
    fn current_position(&self)
    -> impl Future<Output = Result<Option<Position>, StoreError>> + Send;
}

impl<T: GlobalFeed> GlobalFeed for std::sync::Arc<T> {
    fn read_all(
        &self,
        from: Option<&Position>,
        options: FeedReadOptions,
    ) -> impl Future<Output = Result<Vec<EventEnvelope>, StoreError>> + Send {
        (**self).read_all(from, options)
    }

    fn current_position(
        &self,
    ) -> impl Future<Output = Result<Option<Position>, StoreError>> + Send {
        (**self).current_position()
    }
}

impl<T: AggregateStore> AggregateStore for std::sync::Arc<T> {
    fn append(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
    ) -> impl Future<Output = Result<AppendResult, StoreError>> + Send {
        (**self).append(stream, events, expected)
    }

    fn read_stream(
        &self,
        stream: &StreamId,
        options: StreamReadOptions,
    ) -> impl Future<Output = Result<Vec<EventEnvelope>, StoreError>> + Send {
        (**self).read_stream(stream, options)
    }

    fn stream_version(
        &self,
        stream: &StreamId,
    ) -> impl Future<Output = Result<Version, StoreError>> + Send {
        (**self).stream_version(stream)
    }
}
