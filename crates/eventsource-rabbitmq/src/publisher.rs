//! [`EventPublisher`] that publishes envelopes to a `RabbitMQ` topic exchange.

use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::error::BusError;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use tokio::sync::Mutex;

use crate::config::RabbitMqConfig;
use crate::wire;

/// Publishes envelopes as persistent messages, waiting for the broker to confirm each batch.
pub struct RabbitMqPublisher {
    url: String,
    config: RabbitMqConfig,
    channel: Mutex<Option<(Connection, Channel)>>,
}

impl RabbitMqPublisher {
    /// A publisher for the broker at `url`. Connects lazily, on first publish.
    #[must_use]
    pub fn new(url: &str, config: RabbitMqConfig) -> Self {
        Self {
            url: url.to_owned(),
            config,
            channel: Mutex::new(None),
        }
    }

    async fn open(&self) -> Result<(Connection, Channel), lapin::Error> {
        let connection = Connection::connect(&self.url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;
        channel
            .exchange_declare(
                self.config.exchange.as_str().into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        Ok((connection, channel))
    }

    async fn channel(&self) -> Result<Channel, lapin::Error> {
        let mut slot = self.channel.lock().await;
        if let Some((_, channel)) = slot.as_ref() {
            return Ok(channel.clone());
        }
        let (connection, channel) = self.open().await?;
        *slot = Some((connection, channel.clone()));
        Ok(channel)
    }
}

impl EventPublisher for RabbitMqPublisher {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        let channel = self.channel().await.map_err(backend)?;
        let mut confirms = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            let properties = BasicProperties::default()
                .with_delivery_mode(2)
                .with_content_type("application/json".into())
                .with_message_id(envelope.record.meta.event_id.to_string().into())
                .with_type(envelope.record.event_type.as_str().into());
            let confirm = channel
                .basic_publish(
                    self.config.exchange.as_str().into(),
                    envelope.record.event_type.as_str().into(),
                    BasicPublishOptions::default(),
                    &wire::encode(envelope),
                    properties,
                )
                .await
                .map_err(backend)?;
            confirms.push(confirm);
        }
        for confirm in confirms {
            confirm.await.map_err(backend)?;
        }
        Ok(())
    }
}

fn backend(error: lapin::Error) -> BusError {
    BusError::Backend(Box::new(error))
}
