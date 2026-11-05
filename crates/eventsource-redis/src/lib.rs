//! Redis Streams adapter for the event bus: a publisher and a stream reader.

mod config;
mod publisher;
mod wire;

pub use config::RedisBusConfig;
pub use publisher::RedisPublisher;
