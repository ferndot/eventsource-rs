//! Transactional outbox port: messages stored for later, at-least-once publishing.
//!
//! An outbox message is written in the same unit of work as the events it
//! announces (see [`crate::repository::AggregateRepository::save_with_outbox`]).
//! A relay then drains pending messages to a publisher, so a crash between the
//! commit and the publish loses nothing.

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use crate::error::OutboxError;
use crate::stream_id::StreamId;

/// Identifies one outbox message. Stores assign these, in enqueue order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OutboxId(u64);

impl OutboxId {
    /// Wraps a raw id. Stores assign these.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Where a message is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxStatus {
    /// Not yet delivered. The relay will pick it up.
    Pending,
    /// Delivered. The relay will not pick it up again.
    Published,
}

/// A message to enqueue.
#[derive(Debug, Clone, PartialEq)]
pub struct NewOutboxMessage {
    /// Topic or queue the message is for. Must not be empty.
    pub destination: String,
    /// The message body.
    pub payload: Value,
    /// The stream whose events this message announces, if any.
    pub stream: Option<StreamId>,
}

impl NewOutboxMessage {
    /// A message for `destination` that refers to no stream.
    #[must_use]
    pub fn new(destination: impl Into<String>, payload: Value) -> Self {
        Self {
            destination: destination.into(),
            payload,
            stream: None,
        }
    }

    /// Records the stream this message announces.
    #[must_use]
    pub fn for_stream(mut self, stream: StreamId) -> Self {
        self.stream = Some(stream);
        self
    }
}

/// An outbox message as held by a store.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxMessage {
    /// Id to pass to the store's `mark_*` methods.
    pub id: OutboxId,
    /// Topic or queue the message is for.
    pub destination: String,
    /// The message body.
    pub payload: Value,
    /// The stream whose events this message announces, if any.
    pub stream: Option<StreamId>,
    /// How many delivery attempts have failed.
    pub attempts: u32,
    /// Where the message is in its life.
    pub status: OutboxStatus,
}

/// Durable storage for outbox messages.
pub trait OutboxStore: Send + Sync {
    /// Stores `message` as pending and returns its id.
    ///
    /// # Errors
    /// [`OutboxError::Invalid`] if the message is malformed (empty destination),
    /// [`OutboxError::Backend`] if the backend fails.
    fn enqueue(
        &self,
        message: NewOutboxMessage,
    ) -> impl Future<Output = Result<OutboxId, OutboxError>> + Send;

    /// Up to `limit` pending messages, oldest first.
    ///
    /// # Errors
    /// [`OutboxError::Backend`] if the backend fails.
    fn fetch_pending(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, OutboxError>> + Send;
}

impl<T: OutboxStore> OutboxStore for Arc<T> {
    fn enqueue(
        &self,
        message: NewOutboxMessage,
    ) -> impl Future<Output = Result<OutboxId, OutboxError>> + Send {
        (**self).enqueue(message)
    }

    fn fetch_pending(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, OutboxError>> + Send {
        (**self).fetch_pending(limit)
    }
}
