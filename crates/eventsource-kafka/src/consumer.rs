//! Consumer side: reads envelopes back from a Kafka topic.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use eventsource_core::envelope::EventEnvelope;
use eventsource_core::position::Position;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio::time::Instant;

use crate::config::KafkaConsumerConfig;
use crate::wire::{self, DecodeRecordError};

/// How long to wait for more records once a batch has started filling.
const DRAIN_WAIT: Duration = Duration::from_millis(50);

/// One record read from the topic.
#[derive(Debug, Clone)]
pub struct Delivery {
    /// The decoded envelope. Its position is scoped to this consumer, see [`KafkaConsumer`].
    pub envelope: EventEnvelope,
    /// The partition the record came from.
    pub partition: i32,
    /// The record's offset in that partition.
    pub offset: i64,
}

/// Why a poll failed.
#[derive(Debug, thiserror::Error)]
pub enum ConsumeError {
    /// Kafka reported an error.
    #[error("kafka consumer failed")]
    Kafka(#[from] KafkaError),
    /// A record is not a valid envelope.
    #[error("undecodable record")]
    Decode(#[from] DecodeRecordError),
}

/// Reads the envelopes a [`crate::KafkaPublisher`] wrote to one topic, as a member of a consumer group.
pub struct KafkaConsumer {
    consumer: Arc<StreamConsumer>,
    topic: String,
    store_id: String,
    next_token: AtomicU64,
}

impl KafkaConsumer {
    /// A consumer subscribed to the topic in `config`. Starts at the earliest record when the group has no committed offset.
    ///
    /// # Errors
    /// If librdkafka rejects the configuration or the subscription.
    pub fn new(config: &KafkaConsumerConfig) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[config.topic.as_str()])?;
        Ok(Self {
            consumer: Arc::new(consumer),
            topic: config.topic.clone(),
            store_id: format!("kafka:{}:{}", config.topic, config.group_id),
            next_token: AtomicU64::new(1),
        })
    }

    /// Waits up to `wait` for a first record, then returns it with whatever else is ready, up to `max`.
    /// Empty if nothing arrived in time.
    ///
    /// # Errors
    /// [`ConsumeError`] if Kafka fails or a record is not a valid envelope.
    pub async fn poll_batch(
        &self,
        max: NonZeroUsize,
        wait: Duration,
    ) -> Result<Vec<Delivery>, ConsumeError> {
        let deadline = Instant::now() + wait;
        let mut out = Vec::new();
        while out.len() < max.get() {
            let timeout = if out.is_empty() {
                deadline.saturating_duration_since(Instant::now())
            } else {
                DRAIN_WAIT
            };
            let Ok(received) = tokio::time::timeout(timeout, self.consumer.recv()).await else {
                break;
            };
            let message = received?;
            let wire = wire::decode(message.partition(), message.offset(), message.payload())?;
            let token = self.next_token.fetch_add(1, Ordering::Relaxed);
            out.push(Delivery {
                envelope: wire.into_envelope(Position::new(self.store_id.clone(), token)),
                partition: message.partition(),
                offset: message.offset(),
            });
        }
        Ok(out)
    }

    /// Commits the offsets of `deliveries`: for each partition, everything up to and including the highest offset given.
    ///
    /// Call this only after the deliveries were handled. Anything not committed is read again by the next
    /// consumer of the group, so delivery is at-least-once. Waits for the broker to acknowledge.
    ///
    /// # Errors
    /// If the commit fails. Nothing is then known to be committed.
    pub async fn commit(&self, deliveries: &[Delivery]) -> Result<(), KafkaError> {
        let mut next: BTreeMap<i32, i64> = BTreeMap::new();
        for delivery in deliveries {
            let slot = next.entry(delivery.partition).or_insert(0);
            *slot = (*slot).max(delivery.offset + 1);
        }
        if next.is_empty() {
            return Ok(());
        }
        let mut list = TopicPartitionList::new();
        for (partition, offset) in next {
            list.add_partition_offset(&self.topic, partition, Offset::Offset(offset))?;
        }
        let consumer = Arc::clone(&self.consumer);
        tokio::task::spawn_blocking(move || consumer.commit(&list, CommitMode::Sync))
            .await
            .map_err(|_| KafkaError::Canceled)?
    }
}
