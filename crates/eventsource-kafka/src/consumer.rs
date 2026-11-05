//! Consumer side: reads envelopes back from a Kafka topic.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventsource_core::envelope::{EventEnvelope, FeedReadOptions};
use eventsource_core::error::StoreError;
use eventsource_core::position::Position;
use eventsource_core::store::GlobalFeed;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio::time::Instant;

use crate::config::KafkaConsumerConfig;
use crate::wire::{self, DecodeRecordError};

/// Records per feed read when the caller sets no limit.
const DEFAULT_PAGE: usize = 100;

/// How often a feed read that is waiting for its partition assignment checks for it.
const ASSIGNMENT_POLL_SLICE: Duration = Duration::from_millis(200);

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
    assignment_wait: Duration,
    feed_poll_wait: Duration,
    assigned: AtomicBool,
    /// Records handed out through the feed and not yet committed: token to (partition, offset).
    pending: Mutex<BTreeMap<u64, (i32, i64)>>,
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
            // Tokens start at the wall clock so they keep growing across restarts of a subscription.
            next_token: AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(1, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX)),
            ),
            assignment_wait: config.assignment_wait,
            feed_poll_wait: config.feed_poll_wait,
            assigned: AtomicBool::new(false),
            pending: Mutex::new(BTreeMap::new()),
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
        self.commit_next_offsets(next).await
    }

    /// Commits `next`: for each partition, the offset of the next record to read.
    async fn commit_next_offsets(&self, next: BTreeMap<i32, i64>) -> Result<(), KafkaError> {
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

    /// Polls for a feed page. Until the group has assigned partitions, which only
    /// happens while polling, the wait is `assignment_wait`; afterwards `feed_poll_wait`.
    async fn poll_for_feed(&self, limit: NonZeroUsize) -> Result<Vec<Delivery>, ConsumeError> {
        let mut deadline = Instant::now()
            + if self.assigned.load(Ordering::Relaxed) {
                self.feed_poll_wait
            } else {
                self.assignment_wait
            };
        loop {
            let slice = deadline
                .saturating_duration_since(Instant::now())
                .min(ASSIGNMENT_POLL_SLICE);
            let deliveries = self.poll_batch(limit, slice).await?;
            if !deliveries.is_empty() || Instant::now() >= deadline {
                return Ok(deliveries);
            }
            if !self.assigned.load(Ordering::Relaxed)
                && self.consumer.assignment().is_ok_and(|a| a.count() > 0)
            {
                self.assigned.store(true, Ordering::Relaxed);
                deadline = deadline.min(Instant::now() + self.feed_poll_wait);
            }
        }
    }
}

/// A tenant or category filter was given to a feed that cannot apply it.
#[derive(Debug, thiserror::Error)]
#[error("the Kafka feed cannot filter by tenant or category")]
pub struct UnsupportedFilter;

fn backend(error: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    StoreError::Backend(Box::new(error))
}

/// The consumer as a feed for a [`eventsource_core::subscription::SubscriptionRunner`].
///
/// The feed follows the consumer group, not a position. `read_all` ignores which
/// records `from` names and returns what the group has not read yet; what `from`
/// does is acknowledge: when a runner passes the position of the last page it
/// delivered and checkpointed, every record up to it is committed to Kafka
/// before the next page is read. So a crash between delivery and the next read
/// redelivers that page, which is at-least-once. Positions are scoped to
/// `kafka:<topic>:<group>` and only order records handed out by one consumer
/// instance and its successors; they say nothing about Kafka offsets.
impl GlobalFeed for KafkaConsumer {
    async fn read_all(
        &self,
        from: Option<&Position>,
        options: FeedReadOptions,
    ) -> Result<Vec<EventEnvelope>, StoreError> {
        if options.tenant_id.is_some() || options.category.is_some() {
            return Err(backend(UnsupportedFilter));
        }
        if let Some(from) = from {
            from.try_cmp(&Position::new(self.store_id.clone(), 0))?;
            let acknowledged = {
                let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
                let rest = pending.split_off(&from.token().saturating_add(1));
                std::mem::replace(&mut *pending, rest)
            };
            let mut next: BTreeMap<i32, i64> = BTreeMap::new();
            for &(partition, offset) in acknowledged.values() {
                let slot = next.entry(partition).or_insert(0);
                *slot = (*slot).max(offset + 1);
            }
            if let Err(error) = self.commit_next_offsets(next).await {
                self.pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .extend(acknowledged);
                return Err(backend(error));
            }
        }
        let limit = options
            .limit
            .unwrap_or(NonZeroUsize::new(DEFAULT_PAGE).unwrap_or(NonZeroUsize::MIN));
        let deliveries = self.poll_for_feed(limit).await.map_err(backend)?;
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        for delivery in &deliveries {
            pending.insert(
                delivery.envelope.position.token(),
                (delivery.partition, delivery.offset),
            );
        }
        Ok(deliveries.into_iter().map(|d| d.envelope).collect())
    }

    /// Always `None`: a Kafka group has no store-wide newest position. Where a new
    /// group starts is decided by `auto.offset.reset`, which is `earliest`.
    async fn current_position(&self) -> Result<Option<Position>, StoreError> {
        Ok(None)
    }
}
