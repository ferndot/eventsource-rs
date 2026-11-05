//! Consumer side: reads a `RabbitMQ` queue as a [`GlobalFeed`].

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use eventsource_core::envelope::{EventEnvelope, FeedReadOptions};
use eventsource_core::error::{PositionError, StoreError};
use eventsource_core::position::Position;
use eventsource_core::store::GlobalFeed;
use lapin::options::{
    BasicAckOptions, BasicGetOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, ExchangeKind};

use crate::config::RabbitMqConfig;
use crate::wire;

/// Messages fetched per read when the caller sets no limit.
const PAGE: usize = 100;

/// Reads the events a [`crate::RabbitMqPublisher`] sent to a queue.
pub struct RabbitMqConsumer {
    connection: Connection,
    channel: Channel,
    queue: String,
    store_id: String,
    state: Mutex<Delivered>,
}

/// What has been handed out and not yet acknowledged.
struct Delivered {
    /// The token before the first one this instance hands out.
    started: u64,
    /// The token of the last envelope handed out.
    last: u64,
    /// Token to broker delivery tag, for envelopes read but not acknowledged.
    pending: BTreeMap<u64, u64>,
}

impl RabbitMqConsumer {
    /// Connects, declares the exchange and queue, and binds the queue.
    ///
    /// # Errors
    /// If the broker is unreachable or refuses the topology.
    pub async fn connect(url: &str, config: RabbitMqConfig) -> Result<Self, lapin::Error> {
        let connection = Connection::connect(url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        channel
            .exchange_declare(
                config.exchange.as_str().into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await?;
        channel
            .queue_declare(
                config.queue.as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await?;
        for binding in &config.bindings {
            channel
                .queue_bind(
                    config.queue.as_str().into(),
                    config.exchange.as_str().into(),
                    binding.as_str().into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;
        }
        let start = start_token();
        Ok(Self {
            connection,
            channel,
            queue: config.queue.clone(),
            store_id: format!("rabbitmq:{}", config.queue),
            state: Mutex::new(Delivered {
                started: start,
                last: start,
                pending: BTreeMap::new(),
            }),
        })
    }

    /// Acknowledges every delivery up to and including `position`.
    ///
    /// Positions from another consumer instance match nothing and are ignored,
    /// so a stale checkpoint cannot acknowledge a message it never handled.
    async fn acknowledge_through(&self, position: &Position) -> Result<(), StoreError> {
        if position.store_id() != self.store_id {
            return Err(PositionError::Foreign {
                left: position.store_id().to_owned(),
                right: self.store_id.clone(),
            }
            .into());
        }
        let tags: Vec<u64> = {
            let mut state = lock_state(&self.state);
            let later = state.pending.split_off(&(position.token() + 1));
            std::mem::replace(&mut state.pending, later)
                .into_values()
                .collect()
        };
        for tag in tags {
            self.channel
                .basic_ack(tag, BasicAckOptions::default())
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    /// Closes the connection. Messages delivered but not acknowledged go back on the queue.
    ///
    /// # Errors
    /// If the broker fails the close.
    pub async fn close(&self) -> Result<(), lapin::Error> {
        self.connection.close(200, "closing".into()).await
    }
}

/// Tokens are scoped to one consumer instance: `unix ms << 20`, then counting up.
/// A checkpoint left by an earlier instance therefore never matches a delivery of this one.
fn start_token() -> u64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0));
    ms << 20
}

/// Locks the delivery state. A panic in another holder does not make the state
/// unusable: every update leaves it consistent, so the poison flag is ignored.
fn lock_state(state: &Mutex<Delivered>) -> MutexGuard<'_, Delivered> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A queue cannot be filtered by tenant or category without consuming the rest.
#[derive(Debug, thiserror::Error)]
#[error("a RabbitMQ queue cannot be filtered by tenant or category; bind by routing key instead")]
struct UnsupportedFilter;

fn backend(error: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::Backend(Box::new(error))
}

impl GlobalFeed for RabbitMqConsumer {
    async fn read_all(
        &self,
        from: Option<&Position>,
        options: FeedReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        if options.tenant_id.is_some() || options.category.is_some() {
            return Err(backend(UnsupportedFilter));
        }
        if let Some(position) = from {
            self.acknowledge_through(position).await?;
        }
        let want = options.limit.map_or(PAGE, std::num::NonZeroUsize::get);
        let mut out = Vec::new();
        while out.len() < want {
            let got = self
                .channel
                .basic_get(
                    self.queue.as_str().into(),
                    BasicGetOptions { no_ack: false },
                )
                .await
                .map_err(backend)?;
            let Some(message) = got else { break };
            let token = {
                let mut state = lock_state(&self.state);
                state.last += 1;
                let token = state.last;
                state.pending.insert(token, message.delivery.delivery_tag);
                token
            };
            let position = Position::new(self.store_id.clone(), token);
            out.push(wire::decode(&message.delivery.data, position).map_err(backend)?);
        }
        Ok(out)
    }

    async fn current_position(&self) -> Result<Option<Position>, StoreError> {
        let state = lock_state(&self.state);
        let handed_out = state.last > state.started;
        Ok(handed_out.then(|| Position::new(self.store_id.clone(), state.last)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn poisoned() -> Arc<Mutex<Delivered>> {
        let state = Arc::new(Mutex::new(Delivered {
            started: 10,
            last: 12,
            pending: BTreeMap::from([(11, 1), (12, 2)]),
        }));
        let holder = Arc::clone(&state);
        let result = std::thread::spawn(move || {
            let _guard = holder.lock().unwrap();
            panic!("poison the lock");
        })
        .join();
        assert!(result.is_err());
        assert!(state.is_poisoned());
        state
    }

    #[test]
    fn a_poisoned_lock_does_not_panic_the_next_operation() {
        let state = poisoned();
        let guard = lock_state(&state);
        assert_eq!(guard.last, 12);
        assert_eq!(guard.pending.len(), 2);
    }

    #[test]
    fn a_poisoned_lock_still_allows_mutation() {
        let state = poisoned();
        lock_state(&state).last += 1;
        assert_eq!(lock_state(&state).last, 13);
    }
}
