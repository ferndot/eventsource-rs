//! RabbitMQ adapter for the event bus: a publisher and a queue consumer.

mod config;
mod consumer;
mod publisher;
mod wire;

pub use config::RabbitMqConfig;
pub use consumer::RabbitMqConsumer;
pub use publisher::RabbitMqPublisher;
