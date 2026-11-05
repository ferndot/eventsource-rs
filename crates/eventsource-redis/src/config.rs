//! Settings for the Redis bus.

/// Where events are published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisBusConfig {
    /// Name of the Redis Stream (the key) events are appended to.
    pub stream: String,
    /// Cap on stream length, enforced exactly (`XADD MAXLEN`) on every append; `None` keeps everything.
    pub max_len: Option<usize>,
}

impl RedisBusConfig {
    /// A config publishing to `stream`, with no length cap.
    #[must_use]
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            max_len: None,
        }
    }
}
