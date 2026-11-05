//! Transactional outbox port: messages stored for later, at-least-once publishing.
//!
//! An outbox message is written in the same unit of work as the events it
//! announces (see [`crate::repository::AggregateRepository::save_with_outbox`]).
//! A relay then drains pending messages to a publisher, so a crash between the
//! commit and the publish loses nothing.

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use crate::envelope::AppendResult;
use crate::error::{AtomicAppendError, BackendError, OutboxError};
use crate::event::EventRecord;
use crate::store::AggregateStore;
use crate::stream_id::StreamId;
use crate::version::ExpectedVersion;

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

impl NewOutboxMessage {
    /// Checks the message is storable. Stores call this before writing.
    ///
    /// # Errors
    /// [`OutboxError::Invalid`] if the destination is blank.
    pub fn validate(&self) -> Result<(), OutboxError> {
        if self.destination.trim().is_empty() {
            return Err(OutboxError::Invalid("destination is empty".to_owned()));
        }
        Ok(())
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

    /// Marks a message published, so it is no longer fetched as pending.
    /// Marking an unknown or already published message is a no-op.
    ///
    /// # Errors
    /// [`OutboxError::Backend`] if the backend fails.
    fn mark_published(&self, id: OutboxId) -> impl Future<Output = Result<(), OutboxError>> + Send;

    /// Records a failed delivery: the attempt count goes up by one and the
    /// message stays pending. Unknown ids are a no-op.
    ///
    /// # Errors
    /// [`OutboxError::Backend`] if the backend fails.
    fn mark_failed(
        &self,
        id: OutboxId,
        error: &str,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send;
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

    fn mark_published(&self, id: OutboxId) -> impl Future<Output = Result<(), OutboxError>> + Send {
        (**self).mark_published(id)
    }

    fn mark_failed(
        &self,
        id: OutboxId,
        error: &str,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send {
        (**self).mark_failed(id, error)
    }
}

/// An event store that can append events and outbox messages as one unit.
pub trait OutboxAppend: AggregateStore {
    /// Appends `events` to `stream` and enqueues `messages`, atomically.
    ///
    /// Either the events and every message are stored, or nothing is. This is
    /// what lets a relay publish exactly the events that were committed.
    ///
    /// # Errors
    /// [`AtomicAppendError::Store`] as for [`AggregateStore::append`];
    /// [`AtomicAppendError::Outbox`] if a message is invalid or cannot be stored.
    fn append_with_outbox(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
        messages: Vec<NewOutboxMessage>,
    ) -> impl Future<Output = Result<AppendResult, AtomicAppendError>> + Send;
}

impl<T: OutboxAppend> OutboxAppend for Arc<T> {
    fn append_with_outbox(
        &self,
        stream: &StreamId,
        events: Vec<EventRecord>,
        expected: ExpectedVersion,
        messages: Vec<NewOutboxMessage>,
    ) -> impl Future<Output = Result<AppendResult, AtomicAppendError>> + Send {
        (**self).append_with_outbox(stream, events, expected, messages)
    }
}

/// Delivers outbox messages to their destination, for example a message bus.
pub trait OutboxPublisher: Send + Sync {
    /// Delivers one message. `Ok` means the destination has accepted it.
    ///
    /// # Errors
    /// Any error leaves the message pending, to be offered again.
    fn publish(
        &self,
        message: &OutboxMessage,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;
}

/// One message the relay could not deliver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayFailure {
    /// The message that stayed pending.
    pub id: OutboxId,
    /// Text of the publisher's error.
    pub error: String,
}

/// What one [`relay`] run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelayReport {
    /// How many messages were delivered and marked published.
    pub published: usize,
    /// The message that failed, if the run stopped on one.
    pub failed: Vec<RelayFailure>,
}

/// Drains up to `limit` pending messages from `store` into `publisher`.
///
/// A message is marked published only after `publisher` accepts it, so a crash
/// between the two re-delivers it on the next run: delivery is at-least-once
/// and consumers must tolerate duplicates.
///
/// # Errors
/// [`OutboxError`] if the store fails.
pub async fn relay<S, P>(store: &S, publisher: &P, limit: usize) -> Result<RelayReport, OutboxError>
where
    S: OutboxStore,
    P: OutboxPublisher,
{
    let mut report = RelayReport::default();
    for message in store.fetch_pending(limit).await? {
        match publisher.publish(&message).await {
            Ok(()) => {
                store.mark_published(message.id).await?;
                report.published += 1;
            }
            Err(error) => {
                let error = error.to_string();
                store.mark_failed(message.id, &error).await?;
                report.failed.push(RelayFailure {
                    id: message.id,
                    error,
                });
                break;
            }
        }
    }
    Ok(report)
}
