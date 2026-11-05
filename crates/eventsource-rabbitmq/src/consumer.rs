//! Consumer side: reads a RabbitMQ queue as a [`GlobalFeed`].

use std::sync::atomic::{AtomicU64, Ordering};

use eventsource_core::envelope::{EventEnvelope, FeedReadOptions};
use eventsource_core::error::StoreError;
use eventsource_core::position::Position;
use eventsource_core::store::GlobalFeed;
use lapin::options::{BasicGetOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
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
    next: AtomicU64,
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
        Ok(Self {
            connection,
            channel,
            queue: config.queue.clone(),
            store_id: format!("rabbitmq:{}", config.queue),
            next: AtomicU64::new(0),
        })
    }

    /// Closes the connection. Messages delivered but not acknowledged go back on the queue.
    ///
    /// # Errors
    /// If the broker fails the close.
    pub async fn close(&self) -> Result<(), lapin::Error> {
        self.connection.close(200, "closing".into()).await
    }
}

fn backend(error: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::Backend(Box::new(error))
}

impl GlobalFeed for RabbitMqConsumer {
    async fn read_all(
        &self,
        _from: Option<&Position>,
        options: FeedReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        let want = options.limit.map_or(PAGE, std::num::NonZeroUsize::get);
        let mut out = Vec::new();
        while out.len() < want {
            let got = self
                .channel
                .basic_get(
                    self.queue.as_str().into(),
                    BasicGetOptions { no_ack: true },
                )
                .await
                .map_err(backend)?;
            let Some(message) = got else { break };
            let token = self.next.fetch_add(1, Ordering::Relaxed) + 1;
            let position = Position::new(self.store_id.clone(), token);
            out.push(wire::decode(&message.delivery.data, position).map_err(backend)?);
        }
        Ok(out)
    }

    async fn current_position(&self) -> Result<Option<Position>, StoreError> {
        let token = self.next.load(Ordering::Relaxed);
        Ok((token > 0).then(|| Position::new(self.store_id.clone(), token)))
    }
}
