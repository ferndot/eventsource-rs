//! [`EventPublisher`] that appends envelopes to a Redis Stream.

use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::error::BusError;

use crate::config::RedisBusConfig;
use crate::connection::LazyConnection;
use crate::wire;

/// Appends published envelopes to a Redis Stream, in order.
pub struct RedisPublisher {
    connection: LazyConnection,
    config: RedisBusConfig,
}

impl RedisPublisher {
    /// A publisher for the Redis at `url`. Connects lazily, on first publish.
    ///
    /// # Errors
    /// If `url` is not a valid Redis URL.
    pub fn new(url: &str, config: RedisBusConfig) -> Result<Self, redis::RedisError> {
        Ok(Self {
            connection: LazyConnection::open(url)?,
            config,
        })
    }
}

impl EventPublisher for RedisPublisher {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        let mut pipe = redis::pipe();
        pipe.atomic();
        for envelope in envelopes {
            let fields = wire::encode(envelope);
            pipe.xadd(&self.config.stream, "*", &fields).ignore();
        }
        let mut conn = self.connection.get().await.map_err(backend)?;
        pipe.query_async::<()>(&mut conn).await.map_err(backend)
    }
}

fn backend(error: redis::RedisError) -> BusError {
    BusError::Backend(Box::new(error))
}
