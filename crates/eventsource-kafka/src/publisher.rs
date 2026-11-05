//! [`EventPublisher`] that produces envelopes to a Kafka topic.

use eventsource_core::bus::EventPublisher;
use eventsource_core::envelope::EventEnvelope;
use eventsource_core::error::BusError;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::config::KafkaBusConfig;
use crate::wire;

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
            .create()?;
        Ok(Self { producer, config })
    }
}

impl EventPublisher for KafkaPublisher {
    async fn publish(&self, envelopes: &[EventEnvelope]) -> Result<(), BusError> {
        for envelope in envelopes {
            let key = envelope.stream.to_string();
            let value = wire::encode(envelope);
            let record = FutureRecord::to(&self.config.topic)
                .key(&key)
                .payload(&value);
            self.producer
                .send(record, Timeout::Never)
                .await
                .map_err(|(error, _)| backend(error))?;
        }
        Ok(())
    }
}

fn backend(error: KafkaError) -> BusError {
    BusError::Backend(Box::new(error))
}
