//! Settings for the RabbitMQ bus.

/// Where events are published and which queue a consumer reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RabbitMqConfig {
    /// Name of the durable topic exchange events are published to.
    pub exchange: String,
    /// Name of the durable queue a consumer reads.
    pub queue: String,
    /// Routing-key patterns the queue is bound with. Defaults to `#` (everything).
    pub bindings: Vec<String>,
}

impl RabbitMqConfig {
    /// A config for `exchange` and `queue`, bound to every routing key.
    #[must_use]
    pub fn new(exchange: impl Into<String>, queue: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            queue: queue.into(),
            bindings: vec!["#".to_owned()],
        }
    }
}
