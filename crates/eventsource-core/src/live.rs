//! Live streams of new events from the global feed.

use std::sync::Arc;

use futures::stream::BoxStream;

use crate::envelope::EventEnvelope;

/// A source of live events from the global feed.
///
/// The stream yields only events appended after [`Self::subscribe_live`] was
/// called, in feed order, and waits for new appends rather than polling.
/// Dropping the stream unsubscribes; it never affects writers. The stream ends
/// if the backend can no longer deliver events (for example a failed feed read).
///
/// The method takes the shared handle (`Arc<Self>`) so the stream can be `'static`.
///
/// This is a best-effort tail, not a durable subscription: it has no
/// checkpoint. Use a [`crate::subscription::SubscriptionRunner`] for
/// at-least-once delivery.
pub trait LiveFeed: Send + Sync + 'static {
    /// Starts a stream of events appended after this call.
    fn subscribe_live(self: Arc<Self>) -> BoxStream<'static, EventEnvelope>;
}
