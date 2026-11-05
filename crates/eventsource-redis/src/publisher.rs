//! [`EventPublisher`] that appends envelopes to a Redis Stream.

use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::error::BusError;
use redis::aio::MultiplexedConnection;
use tokio::sync::Mutex;

use crate::config::RedisBusConfig;
use crate::wire;

/// Appends published envelopes to a Redis Stream, in order.
pub struct RedisPublisher {
    client: redis::Client,
    config: RedisBusConfig,
    connection: Mutex<Option<MultiplexedConnection>>,
}

impl RedisPublisher {
    /// A publisher for the Redis at `url`. Connects lazily, on first publish.
    ///
    /// # Errors
    /// If `url` is not a valid Redis URL.
    pub fn new(url: &str, config: RedisBusConfig) -> Result<Self, redis::RedisError> {
        Ok(Self {
            client: redis::Client::open(url)?,
            config,
            connection: Mutex::new(None),
        })
    }

    async fn connection(&self) -> Result<MultiplexedConnection, redis::RedisError> {
        let mut slot = self.connection.lock().await;
        if let Some(conn) = slot.as_ref() {
            return Ok(conn.clone());
        }
        let conn = self.client.get_multiplexed_async_connection().await?;
        *slot = Some(conn.clone());
        Ok(conn)
    }
}

impl EventPublisher for RedisPublisher {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        let mut pipe = redis::pipe();
        pipe.atomic();
        for envelope in envelopes {
            pipe.xadd(&self.config.stream, "*", &wire::encode(envelope))
                .ignore();
        }
        // Placeholder error mapping; test 4 drives the real one.
        let mut conn = self
            .connection()
            .await
            .map_err(|_| BusError::HandlerFailures(Vec::new()))?;
        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|_| BusError::HandlerFailures(Vec::new()))
    }
}
