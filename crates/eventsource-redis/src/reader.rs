//! Consumer side: reads a Redis Stream back as a [`GlobalFeed`].

use eventsource_core::envelope::{EventEnvelope, FeedReadOptions};
use eventsource_core::error::{PositionError, StoreError};
use eventsource_core::position::Position;
use eventsource_core::store::GlobalFeed;
use redis::AsyncCommands;
use redis::streams::StreamRangeReply;

use crate::connection::LazyConnection;
use crate::wire::{self, DecodeEntryError, ENVELOPE_FIELD};

/// Entries fetched per round trip when the caller sets no limit.
const PAGE: usize = 100;

/// Reads the events a [`crate::RedisPublisher`] wrote to one stream.
///
/// Positions are derived from Redis entry ids and scoped to `redis:<stream>`,
/// so they can be checkpointed independently of the source store's positions.
/// A malformed entry makes the read fail; it is not skipped.
pub struct RedisStreamReader {
    connection: LazyConnection,
    stream: String,
    store_id: String,
}

impl RedisStreamReader {
    /// A reader for `stream` on the Redis at `url`. Connects lazily.
    ///
    /// # Errors
    /// If `url` is not a valid Redis URL.
    pub fn new(url: &str, stream: impl Into<String>) -> Result<Self, redis::RedisError> {
        let stream = stream.into();
        Ok(Self {
            connection: LazyConnection::open(url)?,
            store_id: format!("redis:{stream}"),
            stream,
        })
    }

    fn position(&self, id: &str) -> Result<Position, DecodeEntryError> {
        Ok(Position::new(self.store_id.clone(), wire::id_to_token(id)?))
    }

    fn decode(&self, entry: &redis::streams::StreamId) -> Result<EventEnvelope, DecodeEntryError> {
        let text: Option<String> = entry.get(ENVELOPE_FIELD);
        let wire = wire::decode(&entry.id, text.as_deref())?;
        Ok(wire.into_envelope(self.position(&entry.id)?))
    }
}

fn backend(error: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::Backend(Box::new(error))
}

impl GlobalFeed for RedisStreamReader {
    async fn read_all(
        &self,
        from: Option<&Position>,
        options: FeedReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let mut start = match from {
            None => "-".to_owned(),
            Some(position) if position.store_id() == self.store_id => {
                format!("({}", wire::token_to_id(position.token()))
            }
            Some(position) => {
                return Err(PositionError::Foreign {
                    left: position.store_id().to_owned(),
                    right: self.store_id.clone(),
                }
                .into());
            }
        };
        let limit = options.limit.map(std::num::NonZeroUsize::get);
        let mut conn = self.connection.get().await.map_err(backend)?;
        let mut out = Vec::new();
        loop {
            let want = limit.map_or(PAGE, |l| (l - out.len()).min(PAGE));
            let reply: StreamRangeReply = conn
                .xrange_count(&self.stream, &start, "+", want)
                .await
                .map_err(backend)?;
            let Some(last) = reply.ids.last() else {
                return Ok(out);
            };
            start = format!("({}", last.id);
            let fetched = reply.ids.len();
            for entry in &reply.ids {
                let envelope = self.decode(entry).map_err(backend)?;
                let tenant_ok = options
                    .tenant_id
                    .is_none_or(|t| envelope.record.meta.tenant_id == Some(t));
                let category_ok = options
                    .category
                    .as_ref()
                    .is_none_or(|c| envelope.stream.category() == c);
                if tenant_ok && category_ok {
                    out.push(envelope);
                }
            }
            if limit.is_some_and(|l| out.len() >= l) || fetched < want {
                return Ok(out);
            }
        }
    }

    async fn current_position(&self) -> Result<Option<Position>, StoreError> {
        let mut conn = self.connection.get().await.map_err(backend)?;
        let reply: StreamRangeReply = conn
            .xrevrange_count(&self.stream, "+", "-", 1)
            .await
            .map_err(backend)?;
        reply
            .ids
            .first()
            .map(|entry| self.position(&entry.id))
            .transpose()
            .map_err(backend)
    }
}
