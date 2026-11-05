//! Consumer side: reads a RabbitMQ queue as a [`GlobalFeed`].

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use eventsource_core::envelope::{EventEnvelope, FeedReadOptions};
use eventsource_core::error::{PositionError, StoreError};
use eventsource_core::position::Position;
use eventsource_core::store::GlobalFeed;
use lapin::options::{BasicAckOptions, BasicGetOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
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
            let mut state = self.state.lock().expect("state lock is not poisoned");
            let later = state.pending.split_off(&(position.token() + 1));
            std::mem::replace(&mut state.pending, later).into_values().collect()
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

fn backend(error: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::Backend(Box::new(error))
}

impl GlobalFeed for RabbitMqConsumer {
    async fn read_all(
        &self,
        from: Option<&Position>,
        options: FeedReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
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
                let mut state = self.state.lock().expect("state lock is not poisoned");
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
        let state = self.state.lock().expect("state lock is not poisoned");
        let handed_out = state.last > state.started;
        Ok(handed_out.then(|| Position::new(self.store_id.clone(), state.last)))
    }
}
