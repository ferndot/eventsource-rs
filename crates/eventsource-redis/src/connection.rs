//! A lazily opened, shared Redis connection.

use redis::aio::MultiplexedConnection;
use tokio::sync::Mutex;

pub(crate) struct LazyConnection {
    client: redis::Client,
    slot: Mutex<Option<MultiplexedConnection>>,
}

impl LazyConnection {
    pub(crate) fn open(url: &str) -> Result<Self, redis::RedisError> {
        Ok(Self {
            client: redis::Client::open(url)?,
            slot: Mutex::new(None),
        })
    }

    /// A handle to the shared connection, connecting on first use.
    pub(crate) async fn get(&self) -> Result<MultiplexedConnection, redis::RedisError> {
        let mut slot = self.slot.lock().await;
        if let Some(conn) = slot.as_ref() {
            return Ok(conn.clone());
        }
        let conn = self.client.get_multiplexed_async_connection().await?;
        *slot = Some(conn.clone());
        Ok(conn)
    }
}
