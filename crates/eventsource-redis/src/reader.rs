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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Utc;
    use eventsource_core::event::{EventMeta, EventRecord};
    use eventsource_core::ids::{AggregateId, CorrelationId};
    use eventsource_core::stream_id::{Category, StreamId};
    use eventsource_core::version::Version;

    use super::*;

    fn reader() -> RedisStreamReader {
        RedisStreamReader::new("redis://127.0.0.1:1", "events").expect("valid url")
    }

    fn entry(id: &str, envelope: Option<&str>) -> redis::streams::StreamId {
        let mut map = HashMap::new();
        if let Some(text) = envelope {
            map.insert(
                ENVELOPE_FIELD.to_owned(),
                redis::Value::BulkString(text.as_bytes().to_vec()),
            );
        }
        redis::streams::StreamId { id: id.to_owned(), map, ..Default::default() }
    }

    fn sample() -> EventEnvelope {
        let meta = EventMeta::new(Utc::now(), CorrelationId::new());
        EventEnvelope {
            record: EventRecord::new(meta, "OrderPlaced", serde_json::json!({ "n": 1 })),
            stream: StreamId::new(AggregateId::new(), Category::new("Order").unwrap()),
            stream_version: Version::new(1),
            position: Position::new("source", 1),
            stored_at: Utc::now(),
        }
    }

    #[test]
    fn decodes_an_entry_into_an_envelope_with_a_redis_position() {
        let original = sample();
        let fields = wire::encode(&original);
        let text = &fields.iter().find(|(n, _)| *n == ENVELOPE_FIELD).unwrap().1;
        let decoded = reader().decode(&entry("12-3", Some(text))).expect("valid entry");
        assert_eq!(decoded.record, original.record);
        assert_eq!(decoded.position, Position::new("redis:events", (12 << 16) | 3));
    }

    #[test]
    fn entry_without_envelope_is_missing_envelope() {
        let error = reader().decode(&entry("12-3", None)).expect_err("must fail");
        assert!(matches!(error, DecodeEntryError::MissingEnvelope { ref id } if id == "12-3"));
    }

    #[test]
    fn garbage_envelope_is_invalid_envelope() {
        let error = reader().decode(&entry("12-3", Some("garbage"))).expect_err("must fail");
        assert!(matches!(error, DecodeEntryError::InvalidEnvelope { .. }));
    }

    #[test]
    fn valid_envelope_with_unusable_entry_id_is_invalid_entry_id() {
        let fields = wire::encode(&sample());
        let text = &fields.iter().find(|(n, _)| *n == ENVELOPE_FIELD).unwrap().1;
        let error = reader().decode(&entry("nonsense", Some(text))).expect_err("must fail");
        assert!(matches!(error, DecodeEntryError::InvalidEntryId { ref id } if id == "nonsense"));
    }
}
