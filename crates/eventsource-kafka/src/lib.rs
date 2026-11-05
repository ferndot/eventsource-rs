//! Kafka adapter for the event bus: a publisher and a partition-ordered consumer.

mod config;
mod consumer;
mod publisher;
mod wire;

pub use config::{KafkaBusConfig, KafkaConsumerConfig};
pub use consumer::{ConsumeError, Delivery, KafkaConsumer};
pub use publisher::KafkaPublisher;
pub use wire::DecodeRecordError;
