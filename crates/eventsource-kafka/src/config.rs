//! Settings for the Kafka publisher and consumer.

use std::time::Duration;

/// Where and how events are published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaBusConfig {
    /// Comma-separated `host:port` list of brokers (`bootstrap.servers`).
    pub bootstrap_servers: String,
    /// Topic every envelope is published to.
    pub topic: String,
    /// How long a record may stay undelivered, retries included, before publish fails (`message.timeout.ms`).
    pub message_timeout: Duration,
}

impl KafkaBusConfig {
    /// A config publishing to `topic` through `bootstrap_servers`.
    #[must_use]
    pub fn new(bootstrap_servers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            message_timeout: Duration::from_secs(30),
        }
    }
}

/// Where and as whom events are consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaConsumerConfig {
    /// Comma-separated `host:port` list of brokers (`bootstrap.servers`).
    pub bootstrap_servers: String,
    /// Topic to read.
    pub topic: String,
    /// Consumer group. Committed offsets are kept per group.
    pub group_id: String,
}

impl KafkaConsumerConfig {
    /// A config reading `topic` as member of `group_id`.
    #[must_use]
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topic: topic.into(),
            group_id: group_id.into(),
        }
    }
}
