//! [`EventPublisher`] that produces envelopes to a Kafka topic.

use std::collections::VecDeque;
use std::time::Duration;

use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::error::BusError;
use futures::channel::oneshot::Canceled;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::types::RDKafkaErrorCode;

use crate::config::KafkaBusConfig;
use crate::wire;

/// How long to wait before retrying when the local queue is full and nothing is in flight.
const QUEUE_FULL_PAUSE: Duration = Duration::from_millis(10);

/// Produces one record per envelope: key is the stream id, value is the envelope as JSON.
pub struct KafkaPublisher {
    producer: FutureProducer,
    config: KafkaBusConfig,
}

impl KafkaPublisher {
    /// A publisher for the cluster in `config`. Brokers are contacted in the background.
    ///
    /// # Errors
    /// If librdkafka rejects the client configuration.
    pub fn new(config: KafkaBusConfig) -> Result<Self, KafkaError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("enable.idempotence", "true")
            .set(
                "message.timeout.ms",
                config.message_timeout.as_millis().to_string(),
            )
            .create()?;
        Ok(Self { producer, config })
    }
}

impl EventPublisher for KafkaPublisher {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        // Enqueue in order, then wait for every ack. Idempotence keeps retries in order.
        let mut pending = VecDeque::new();
        for envelope in envelopes {
            let key = envelope.stream.to_string();
            let value = wire::encode(envelope);
            let mut record = FutureRecord::to(&self.config.topic)
                .key(&key)
                .payload(&value);
            loop {
                match self.producer.send_result(record) {
                    Ok(delivery) => {
                        pending.push_back(delivery);
                        break;
                    }
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                        // Local queue is full: wait for the oldest record to drain, then retry.
                        record = back;
                        match pending.pop_front() {
                            Some(oldest) => acknowledged(oldest).await?,
                            None => tokio::time::sleep(QUEUE_FULL_PAUSE).await,
                        }
                    }
                    Err((error, _)) => return Err(backend(error)),
                }
            }
        }
        for delivery in pending {
            acknowledged(delivery).await?;
        }
        Ok(())
    }
}

async fn acknowledged(delivery: DeliveryFuture) -> Result<(), BusError> {
    match delivery.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err((error, _))) => Err(backend(error)),
        Err(Canceled) => Err(backend(KafkaError::Canceled)),
    }
}

fn backend(error: KafkaError) -> BusError {
    BusError::Backend(Box::new(error))
}
